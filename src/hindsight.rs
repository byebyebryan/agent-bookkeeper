//! A provenance-preserving Hindsight learned-memory consumer.
//!
//! This adapter is intentionally downstream of Bookkeeper's verified lease.
//! It renders only user and assistant Codex messages, sends one replace/upsert
//! request to Hindsight, and writes an idempotent provenance receipt only after
//! that request succeeds. Raw JSONL remains outside Hindsight's ownership.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::controller::PathConsumer;
use crate::delivery::DeliveryLease;
use crate::domain::{CanonicalRevision, DeliveryOutcome, EventKind};

/// A complete, replayable Hindsight retain call derived from one Bookkeeper
/// record revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HindsightRetainRequest {
    pub bank_id: String,
    pub document_id: String,
    pub source_id: String,
    pub content: String,
    pub context: String,
    pub timestamp: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub tags: Vec<String>,
    pub observation_scopes: Option<HindsightObservationScopes>,
}

/// Controls how Hindsight may group source facts during observation
/// consolidation. `None` deliberately leaves Hindsight's server default in
/// effect, preserving existing consumer behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HindsightObservationScopes {
    PerTag,
    Combined,
    AllCombinations,
    Shared,
}

impl HindsightObservationScopes {
    pub const fn name(self) -> &'static str {
        match self {
            Self::PerTag => "per_tag",
            Self::Combined => "combined",
            Self::AllCombinations => "all_combinations",
            Self::Shared => "shared",
        }
    }

    pub fn from_name(value: &str) -> Result<Option<Self>, HindsightConsumerError> {
        match value {
            "default" => Ok(None),
            "per_tag" => Ok(Some(Self::PerTag)),
            "combined" => Ok(Some(Self::Combined)),
            "all_combinations" => Ok(Some(Self::AllCombinations)),
            "shared" => Ok(Some(Self::Shared)),
            _ => Err(HindsightConsumerError::InvalidConfiguration(format!(
                "unknown Hindsight observation scopes {value:?}; expected default, per_tag, combined, all_combinations, or shared"
            ))),
        }
    }
}

/// The subset of a retain result needed in a durable Bookkeeper receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HindsightRetainResponse {
    pub operation_id: Option<String>,
}

/// Executes a single synchronous retain request.
pub trait HindsightRunner {
    fn retain(
        &mut self,
        request: &HindsightRetainRequest,
    ) -> Result<HindsightRetainResponse, String>;
}

/// The Codex transcript rendering contract used before a Hindsight retain call.
///
/// `LegacyV1` is retained for the existing pilot. The two reference profiles
/// are isolated trial modes based on Hindsight's maintained Codex integration:
/// they use `response_item` messages, drop synthetic AGENTS.md startup text and
/// remove recalled-memory echoes. `ReferenceToolAware` additionally preserves
/// bounded structured tool context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HindsightRenderProfile {
    LegacyV1,
    ReferenceMessage,
    ReferenceToolAware,
}

impl HindsightRenderProfile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::LegacyV1 => "legacy-v1",
            Self::ReferenceMessage => "reference-message-v2",
            Self::ReferenceToolAware => "reference-tool-aware-v2",
        }
    }

    pub fn from_name(value: &str) -> Result<Self, HindsightConsumerError> {
        match value {
            "legacy-v1" => Ok(Self::LegacyV1),
            "reference-message-v2" => Ok(Self::ReferenceMessage),
            "reference-tool-aware-v2" => Ok(Self::ReferenceToolAware),
            _ => Err(HindsightConsumerError::InvalidConfiguration(format!(
                "unknown Hindsight render profile {value:?}; expected legacy-v1, reference-message-v2, or reference-tool-aware-v2"
            ))),
        }
    }
}

/// HTTP configuration for a self-hosted Hindsight API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HindsightHttpConfig {
    base_url: String,
    timeout: Duration,
}

impl HindsightHttpConfig {
    pub fn new(
        base_url: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, HindsightConsumerError> {
        let base_url = base_url.into();
        if base_url.is_empty()
            || base_url.trim() != base_url
            || base_url.contains('\0')
            || !(base_url.starts_with("http://") || base_url.starts_with("https://"))
        {
            return Err(HindsightConsumerError::InvalidConfiguration(
                "Hindsight base URL must be a non-empty, trimmed http(s) URL".to_owned(),
            ));
        }
        if timeout.is_zero() {
            return Err(HindsightConsumerError::InvalidConfiguration(
                "Hindsight HTTP timeout must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            timeout,
        })
    }
}

/// Small dependency-free-in-policy HTTP implementation of Hindsight's retain
/// endpoint. The caller supplies only an internal API base URL; no raw source
/// filesystem path is sent to Hindsight.
#[derive(Debug)]
pub struct HindsightHttpRunner {
    config: HindsightHttpConfig,
    agent: ureq::Agent,
}

impl HindsightHttpRunner {
    pub fn new(config: HindsightHttpConfig) -> Self {
        let agent = ureq::AgentBuilder::new().timeout(config.timeout).build();
        Self { config, agent }
    }

    fn endpoint(&self, bank_id: &str) -> String {
        format!(
            "{}/v1/default/banks/{bank_id}/memories",
            self.config.base_url
        )
    }
}

impl HindsightRunner for HindsightHttpRunner {
    fn retain(
        &mut self,
        request: &HindsightRetainRequest,
    ) -> Result<HindsightRetainResponse, String> {
        let mut item = json!({
            "content": request.content,
            "context": request.context,
            "document_id": request.document_id,
            "metadata": request.metadata,
            "tags": request.tags,
            "timestamp": request.timestamp,
            "update_mode": "replace",
        });
        if let Some(observation_scopes) = request.observation_scopes {
            item["observation_scopes"] = Value::String(observation_scopes.name().to_owned());
        }
        let body = serde_json::to_string(&json!({
            "async": false,
            "items": [item],
        }))
        .map_err(|error| format!("could not serialize Hindsight retain request: {error}"))?;
        let response = self
            .agent
            .post(&self.endpoint(&request.bank_id))
            .set("Content-Type", "application/json")
            .send_string(&body)
            .map_err(http_error)?;
        let status = response.status();
        let text = response
            .into_string()
            .map_err(|error| format!("could not read Hindsight response: {error}"))?;
        let value: Value = serde_json::from_str(&text)
            .map_err(|error| format!("Hindsight returned invalid JSON ({status}): {error}"))?;
        if value.get("success").and_then(Value::as_bool) == Some(false) {
            return Err(format!(
                "Hindsight rejected retain request ({status}): {text}"
            ));
        }
        let operation_id = value
            .get("operation_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        Ok(HindsightRetainResponse { operation_id })
    }
}

fn http_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(status, response) => {
            let body = response.into_string().unwrap_or_default();
            format!("Hindsight returned HTTP {status}: {}", body.trim())
        }
        ureq::Error::Transport(error) => format!("could not reach Hindsight: {error}"),
    }
}

/// A durable, idempotent consumer of Codex transcript revisions.
#[derive(Debug)]
pub struct HindsightConsumer<R> {
    receipt_root: PathBuf,
    bank_id: String,
    render_profile: HindsightRenderProfile,
    observation_scopes: Option<HindsightObservationScopes>,
    runner: R,
}

impl<R> HindsightConsumer<R> {
    pub fn new(
        receipt_root: impl Into<PathBuf>,
        bank_id: impl Into<String>,
        runner: R,
    ) -> Result<Self, HindsightConsumerError> {
        Self::new_with_render_profile(
            receipt_root,
            bank_id,
            HindsightRenderProfile::LegacyV1,
            runner,
        )
    }

    pub fn new_with_render_profile(
        receipt_root: impl Into<PathBuf>,
        bank_id: impl Into<String>,
        render_profile: HindsightRenderProfile,
        runner: R,
    ) -> Result<Self, HindsightConsumerError> {
        Self::new_with_render_profile_and_observation_scopes(
            receipt_root,
            bank_id,
            render_profile,
            None,
            runner,
        )
    }

    pub fn new_with_render_profile_and_observation_scopes(
        receipt_root: impl Into<PathBuf>,
        bank_id: impl Into<String>,
        render_profile: HindsightRenderProfile,
        observation_scopes: Option<HindsightObservationScopes>,
        runner: R,
    ) -> Result<Self, HindsightConsumerError> {
        let receipt_root = receipt_root.into();
        if !receipt_root.is_absolute() {
            return Err(HindsightConsumerError::InvalidConfiguration(
                "Hindsight receipt root must be absolute".to_owned(),
            ));
        }
        fs::create_dir_all(&receipt_root)?;
        let metadata = fs::symlink_metadata(&receipt_root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(HindsightConsumerError::InvalidConfiguration(
                "Hindsight receipt root must be a real directory".to_owned(),
            ));
        }
        let bank_id = bank_id.into();
        validate_bank_id(&bank_id)?;
        Ok(Self {
            receipt_root,
            bank_id,
            render_profile,
            observation_scopes,
            runner,
        })
    }

    pub fn receipt_path(&self, delivery: &DeliveryLease) -> PathBuf {
        self.receipt_root
            .join(delivery.subscription_id.as_uuid().hyphenated().to_string())
            .join(format!("{}.json", delivery.event_id.as_uuid().hyphenated()))
    }

    pub fn source_id(delivery: &DeliveryLease) -> String {
        format!("agent-bookkeeper://record/{}", delivery.record_id.as_uuid())
    }
}

impl<R: HindsightRunner> HindsightConsumer<R> {
    fn apply_inner(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, HindsightConsumerError> {
        let payload_revision = match (delivery.revision, payload) {
            (Some(expected), Some(path)) => {
                let actual = CanonicalRevision::from_reader(File::open(path)?)?;
                if actual != expected {
                    return Err(HindsightConsumerError::ProvenanceMismatch);
                }
                Some(actual)
            }
            (Some(_), None) => return Err(HindsightConsumerError::MissingPayload),
            (None, Some(_)) => return Err(HindsightConsumerError::UnexpectedPayload),
            (None, None) => None,
        };
        let source_id = Self::source_id(delivery);
        let destination = self.receipt_path(delivery);
        if destination.exists() {
            let expected = receipt_bytes(
                delivery,
                payload_revision,
                &source_id,
                &self.bank_id,
                self.render_profile,
                self.observation_scopes,
                None,
            );
            let existing = fs::read(&destination)?;
            if receipt_matches(&existing, &expected)? {
                return Ok(receipt_outcome(delivery));
            }
            return Err(HindsightConsumerError::IdempotencyConflict(destination));
        }

        let response = if let Some(revision) = payload_revision {
            let transcript = parse_codex_transcript(
                payload.expect("payload matched a revision"),
                self.render_profile,
            )?;
            let request = retain_request(
                delivery,
                revision,
                &self.bank_id,
                source_id.clone(),
                self.render_profile,
                self.observation_scopes,
                transcript,
            )?;
            Some(
                self.runner
                    .retain(&request)
                    .map_err(HindsightConsumerError::Runner)?,
            )
        } else {
            None
        };
        let receipt = receipt_bytes(
            delivery,
            payload_revision,
            &source_id,
            &self.bank_id,
            self.render_profile,
            self.observation_scopes,
            response
                .as_ref()
                .and_then(|value| value.operation_id.as_deref()),
        );
        write_receipt(&destination, &receipt)?;
        Ok(receipt_outcome(delivery))
    }
}

impl<R: HindsightRunner> PathConsumer for HindsightConsumer<R> {
    fn apply(
        &mut self,
        delivery: &DeliveryLease,
        payload: Option<&Path>,
    ) -> Result<DeliveryOutcome, String> {
        self.apply_inner(delivery, payload)
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug)]
struct CodexTranscript {
    content: String,
    session_id: Option<String>,
    session_cwd: Option<String>,
    last_timestamp: Option<String>,
    render_stats: CodexRenderStats,
}

#[derive(Clone, Debug, Default)]
struct CodexRenderStats {
    source_messages: u64,
    included_messages: u64,
    filtered_synthetic_messages: u64,
    stripped_memory_echoes: u64,
    tool_calls: u64,
    tool_results: u64,
    truncated_tool_results: u64,
}

#[derive(Debug)]
struct RenderedMessage {
    role: &'static str,
    content: Vec<Value>,
    timestamp: Option<String>,
}

const MAX_TOOL_OUTPUT_CHARS: usize = 2_000;

fn parse_codex_transcript(
    path: &Path,
    profile: HindsightRenderProfile,
) -> Result<CodexTranscript, HindsightConsumerError> {
    if profile == HindsightRenderProfile::LegacyV1 {
        return parse_legacy_codex_transcript(path);
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut messages = Vec::new();
    let mut legacy_messages = Vec::new();
    let mut assistant_blocks = Vec::new();
    let mut assistant_timestamp = None;
    let mut saw_reference_message = false;
    let mut session_id = None;
    let mut session_cwd = None;
    let mut stats = CodexRenderStats::default();
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line).map_err(|error| {
            HindsightConsumerError::InvalidTranscript(format!(
                "line {} is not valid JSON: {error}",
                index + 1
            ))
        })?;
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        match value.get("type").and_then(Value::as_str) {
            Some("session_meta") => {
                let payload = value.get("payload").and_then(Value::as_object);
                session_id = payload
                    .and_then(|payload| payload.get("session_id").or_else(|| payload.get("id")))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                session_cwd = payload
                    .and_then(|payload| payload.get("cwd"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
            }
            Some("response_item") => {
                let payload = value.get("payload").and_then(Value::as_object);
                let Some(payload_type) = payload
                    .and_then(|payload| payload.get("type"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                match payload_type {
                    "message" => {
                        let role = match payload
                            .and_then(|payload| payload.get("role"))
                            .and_then(Value::as_str)
                        {
                            Some("user") => "user",
                            Some("assistant") => "assistant",
                            _ => continue,
                        };
                        saw_reference_message = true;
                        stats.source_messages += 1;
                        if role == "assistant"
                            && payload
                                .and_then(|payload| payload.get("phase"))
                                .and_then(Value::as_str)
                                != Some("final_answer")
                        {
                            continue;
                        }
                        let text = payload
                            .and_then(|payload| payload.get("content"))
                            .map(extract_text_from_content_blocks)
                            .unwrap_or_default();
                        let (text, stripped_memory_echo) = strip_memory_tags(&text);
                        if stripped_memory_echo {
                            stats.stripped_memory_echoes += 1;
                        }
                        if role == "user" && is_synthetic_codex_user_message(&text) {
                            stats.filtered_synthetic_messages += 1;
                            continue;
                        }
                        if text.trim().is_empty() {
                            continue;
                        }
                        match profile {
                            HindsightRenderProfile::ReferenceMessage => {
                                messages.push(RenderedMessage {
                                    role,
                                    content: vec![json!({"type": "text", "text": text})],
                                    timestamp: timestamp.clone(),
                                });
                            }
                            HindsightRenderProfile::ReferenceToolAware => {
                                if role == "user" {
                                    flush_assistant(
                                        &mut messages,
                                        &mut assistant_blocks,
                                        &mut assistant_timestamp,
                                    );
                                    messages.push(RenderedMessage {
                                        role: "user",
                                        content: vec![json!({"type": "text", "text": text})],
                                        timestamp: timestamp.clone(),
                                    });
                                } else {
                                    assistant_blocks.push(json!({"type": "text", "text": text}));
                                    assistant_timestamp = timestamp.clone();
                                }
                            }
                            HindsightRenderProfile::LegacyV1 => unreachable!(),
                        }
                    }
                    _ if profile == HindsightRenderProfile::ReferenceToolAware => {
                        append_tool_response_item(
                            payload_type,
                            payload.expect("payload exists"),
                            &mut assistant_blocks,
                            &mut stats,
                        );
                        if !assistant_blocks.is_empty() && timestamp.is_some() {
                            assistant_timestamp = timestamp.clone();
                        }
                    }
                    _ => {}
                }
            }
            Some("event_msg") => {
                let payload = value.get("payload").and_then(Value::as_object);
                let Some(message_type) = payload
                    .and_then(|payload| payload.get("type"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                match message_type {
                    "user_message" | "agent_message" => {
                        let Some(message) = payload
                            .and_then(|payload| payload.get("message"))
                            .and_then(Value::as_str)
                        else {
                            continue;
                        };
                        legacy_messages.push((
                            if message_type == "user_message" {
                                "user"
                            } else {
                                "assistant"
                            },
                            timestamp.clone(),
                            message.to_owned(),
                        ));
                    }
                    _ if profile == HindsightRenderProfile::ReferenceToolAware => {
                        append_tool_event_message(
                            message_type,
                            payload.expect("payload exists"),
                            &mut assistant_blocks,
                            &mut stats,
                        );
                        if !assistant_blocks.is_empty() && timestamp.is_some() {
                            assistant_timestamp = timestamp.clone();
                        }
                    }
                    _ => {}
                }
            }
            _ if value.get("role").is_some() && value.get("content").is_some() => {
                let role = match value.get("role").and_then(Value::as_str) {
                    Some("user") => "user",
                    Some("assistant") => "assistant",
                    _ => continue,
                };
                saw_reference_message = true;
                stats.source_messages += 1;
                let text = value.get("content").map(value_to_text).unwrap_or_default();
                let (text, stripped_memory_echo) = strip_memory_tags(&text);
                if stripped_memory_echo {
                    stats.stripped_memory_echoes += 1;
                }
                if role == "user" && is_synthetic_codex_user_message(&text) {
                    stats.filtered_synthetic_messages += 1;
                    continue;
                }
                if text.trim().is_empty() {
                    continue;
                }
                match profile {
                    HindsightRenderProfile::ReferenceMessage => messages.push(RenderedMessage {
                        role,
                        content: vec![json!({"type": "text", "text": text})],
                        timestamp: timestamp.clone(),
                    }),
                    HindsightRenderProfile::ReferenceToolAware if role == "user" => {
                        flush_assistant(
                            &mut messages,
                            &mut assistant_blocks,
                            &mut assistant_timestamp,
                        );
                        messages.push(RenderedMessage {
                            role: "user",
                            content: vec![json!({"type": "text", "text": text})],
                            timestamp: timestamp.clone(),
                        });
                    }
                    HindsightRenderProfile::ReferenceToolAware => {
                        assistant_blocks.push(json!({"type": "text", "text": text}));
                        assistant_timestamp = timestamp.clone();
                    }
                    HindsightRenderProfile::LegacyV1 => unreachable!(),
                }
            }
            _ => {}
        }
    }
    if profile == HindsightRenderProfile::ReferenceToolAware {
        flush_assistant(
            &mut messages,
            &mut assistant_blocks,
            &mut assistant_timestamp,
        );
    }
    if !saw_reference_message {
        for (role, timestamp, message) in legacy_messages {
            stats.source_messages += 1;
            let (message, stripped_memory_echo) = strip_memory_tags(&message);
            if stripped_memory_echo {
                stats.stripped_memory_echoes += 1;
            }
            if role == "user" && is_synthetic_codex_user_message(&message) {
                stats.filtered_synthetic_messages += 1;
                continue;
            }
            if message.trim().is_empty() {
                continue;
            }
            messages.push(RenderedMessage {
                role,
                content: vec![json!({"type": "text", "text": message})],
                timestamp,
            });
        }
    }
    if messages.is_empty() {
        return Err(HindsightConsumerError::InvalidTranscript(
            "contains no user or assistant messages".to_owned(),
        ));
    }
    stats.included_messages = messages.len() as u64;
    let last_timestamp = messages
        .iter()
        .rev()
        .find_map(|message| message.timestamp.clone());
    let content = serde_json::to_string(
        &messages
            .iter()
            .map(|message| {
                let mut value = json!({"role": message.role, "content": message.content});
                if let Some(timestamp) = &message.timestamp {
                    value["timestamp"] = Value::String(timestamp.clone());
                }
                value
            })
            .collect::<Vec<_>>(),
    )
    .expect("Codex render values serialize without error");
    Ok(CodexTranscript {
        content,
        session_id,
        session_cwd,
        last_timestamp,
        render_stats: stats,
    })
}

fn parse_legacy_codex_transcript(path: &Path) -> Result<CodexTranscript, HindsightConsumerError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut messages = Vec::new();
    let mut session_id = None;
    let mut session_cwd = None;
    let mut last_timestamp = None;
    let mut stats = CodexRenderStats::default();
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line).map_err(|error| {
            HindsightConsumerError::InvalidTranscript(format!(
                "line {} is not valid JSON: {error}",
                index + 1
            ))
        })?;
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        match value.get("type").and_then(Value::as_str) {
            Some("session_meta") => {
                let payload = value.get("payload").and_then(Value::as_object);
                session_id = payload
                    .and_then(|payload| payload.get("session_id").or_else(|| payload.get("id")))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                session_cwd = payload
                    .and_then(|payload| payload.get("cwd"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
            }
            Some("event_msg") => {
                let payload = value.get("payload").and_then(Value::as_object);
                let Some(message_type) = payload
                    .and_then(|payload| payload.get("type"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let role = match message_type {
                    "user_message" => "User",
                    "agent_message" => "Assistant",
                    _ => continue,
                };
                let Some(message) = payload
                    .and_then(|payload| payload.get("message"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if message.trim().is_empty() {
                    continue;
                }
                stats.source_messages += 1;
                let rendered_timestamp = timestamp.as_deref().unwrap_or("unknown time");
                messages.push(format!("{role} ({rendered_timestamp}): {message}"));
                if timestamp.is_some() {
                    last_timestamp = timestamp;
                }
            }
            _ => {}
        }
    }
    if messages.is_empty() {
        return Err(HindsightConsumerError::InvalidTranscript(
            "contains no user or assistant messages".to_owned(),
        ));
    }
    stats.included_messages = messages.len() as u64;
    Ok(CodexTranscript {
        content: messages.join("\n\n"),
        session_id,
        session_cwd,
        last_timestamp,
        render_stats: stats,
    })
}

fn flush_assistant(
    messages: &mut Vec<RenderedMessage>,
    assistant_blocks: &mut Vec<Value>,
    assistant_timestamp: &mut Option<String>,
) {
    if assistant_blocks.is_empty() {
        return;
    }
    messages.push(RenderedMessage {
        role: "assistant",
        content: std::mem::take(assistant_blocks),
        timestamp: assistant_timestamp.take(),
    });
}

fn append_tool_response_item(
    payload_type: &str,
    payload: &serde_json::Map<String, Value>,
    assistant_blocks: &mut Vec<Value>,
    stats: &mut CodexRenderStats,
) {
    match payload_type {
        "local_shell_call" => {
            let command = payload
                .get("action")
                .and_then(Value::as_object)
                .and_then(|action| action.get("command"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            assistant_blocks
                .push(json!({"type": "tool_use", "name": "shell", "input": {"command": command}}));
            stats.tool_calls += 1;
        }
        "function_call" | "custom_tool_call" => {
            let name = payload
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let raw_input = if payload_type == "function_call" {
                payload.get("arguments")
            } else {
                payload.get("input")
            };
            let input = raw_input
                .and_then(Value::as_str)
                .and_then(|value| serde_json::from_str(value).ok())
                .unwrap_or_else(
                    || json!({"raw": raw_input.and_then(Value::as_str).unwrap_or("{}")}),
                );
            assistant_blocks.push(json!({"type": "tool_use", "name": name, "input": input}));
            stats.tool_calls += 1;
        }
        "function_call_output" | "custom_tool_call_output" => {
            let output = payload.get("output").map(extract_function_output_text);
            append_tool_result(output.as_deref(), assistant_blocks, stats);
        }
        "web_search_call" => {
            let query = payload
                .get("action")
                .and_then(Value::as_object)
                .and_then(|action| action.get("query"))
                .and_then(Value::as_str);
            if let Some(query) = query.filter(|query| !query.is_empty()) {
                assistant_blocks.push(
                    json!({"type": "tool_use", "name": "web_search", "input": {"query": query}}),
                );
                stats.tool_calls += 1;
            }
        }
        _ => {}
    }
}

fn append_tool_event_message(
    payload_type: &str,
    payload: &serde_json::Map<String, Value>,
    assistant_blocks: &mut Vec<Value>,
    stats: &mut CodexRenderStats,
) {
    match payload_type {
        "exec_command_end" => {
            let command = payload.get("command").cloned().unwrap_or_else(|| json!([]));
            assistant_blocks
                .push(json!({"type": "tool_use", "name": "shell", "input": {"command": command}}));
            stats.tool_calls += 1;
            let mut result_parts = Vec::new();
            if let Some(output) = payload
                .get("aggregated_output")
                .and_then(Value::as_str)
                .filter(|output| !output.is_empty())
            {
                result_parts.push(output.to_owned());
            }
            if let Some(exit_code) = payload.get("exit_code").and_then(Value::as_i64) {
                if exit_code != 0 {
                    result_parts.push(format!("exit_code: {exit_code}"));
                }
            }
            if let Some(status) = payload
                .get("status")
                .and_then(Value::as_str)
                .filter(|status| *status != "completed")
            {
                result_parts.push(format!("status: {status}"));
            }
            append_tool_result(
                (!result_parts.is_empty())
                    .then(|| result_parts.join("\n"))
                    .as_deref(),
                assistant_blocks,
                stats,
            );
        }
        "patch_apply_end" => {
            let changes = payload.get("changes").cloned().unwrap_or_else(|| json!([]));
            if changes
                .as_array()
                .is_some_and(|changes| !changes.is_empty())
            {
                assistant_blocks.push(
                    json!({"type": "tool_use", "name": "patch", "input": {"changes": changes}}),
                );
                stats.tool_calls += 1;
                if let Some(status) = payload.get("status").and_then(Value::as_str) {
                    append_tool_result(Some(&format!("status: {status}")), assistant_blocks, stats);
                }
            }
        }
        "mcp_tool_call_end" => {
            let result = payload.get("result");
            let result_text = result.and_then(extract_mcp_result_text);
            append_tool_result(result_text.as_deref(), assistant_blocks, stats);
        }
        _ => {}
    }
}

fn append_tool_result(
    output: Option<&str>,
    assistant_blocks: &mut Vec<Value>,
    stats: &mut CodexRenderStats,
) {
    let Some(output) = output.map(str::trim).filter(|output| !output.is_empty()) else {
        return;
    };
    let (output, truncated) = truncate_tool_output(output);
    assistant_blocks.push(json!({"type": "tool_result", "content": output}));
    stats.tool_results += 1;
    if truncated {
        stats.truncated_tool_results += 1;
    }
}

fn extract_text_from_content_blocks(value: &Value) -> String {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter(|block| {
            matches!(
                block.get("type").and_then(Value::as_str),
                Some("input_text") | Some("output_text")
            )
        })
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn extract_function_output_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.trim().to_owned(),
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_object)
            .filter(|item| {
                matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("input_text") | Some("text")
                )
            })
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_owned(),
        _ => String::new(),
    }
}

fn extract_mcp_result_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.to_owned()),
        Value::Object(result) => match result.get("content") {
            Some(Value::String(value)) => Some(value.to_owned()),
            Some(Value::Array(items)) => Some(
                items
                    .iter()
                    .filter_map(Value::as_object)
                    .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
                    .filter_map(|item| item.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            _ => None,
        },
        _ => None,
    }
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.to_owned(),
        Value::Array(_) => extract_text_from_content_blocks(value),
        value => value.to_string(),
    }
}

fn truncate_tool_output(value: &str) -> (String, bool) {
    if value.chars().count() <= MAX_TOOL_OUTPUT_CHARS {
        return (value.to_owned(), false);
    }
    (
        value
            .chars()
            .take(MAX_TOOL_OUTPUT_CHARS)
            .collect::<String>()
            + "... (truncated)",
        true,
    )
}

fn strip_memory_tags(value: &str) -> (String, bool) {
    let mut output = value.to_owned();
    let mut stripped = false;
    for tag in ["hindsight_memories", "relevant_memories"] {
        let start_tag = format!("<{tag}>");
        let end_tag = format!("</{tag}>");
        while let Some(start) = output.find(&start_tag) {
            let Some(end_relative) = output[start + start_tag.len()..].find(&end_tag) else {
                break;
            };
            let end = start + start_tag.len() + end_relative + end_tag.len();
            output.replace_range(start..end, "");
            stripped = true;
        }
    }
    (output, stripped)
}

fn is_synthetic_codex_user_message(value: &str) -> bool {
    let value = value.trim_start();
    value.starts_with("# AGENTS.md instructions for ")
        && value.contains("<INSTRUCTIONS>")
        && value.contains("</INSTRUCTIONS>")
}

fn retain_request(
    delivery: &DeliveryLease,
    revision: CanonicalRevision,
    bank_id: &str,
    source_id: String,
    render_profile: HindsightRenderProfile,
    observation_scopes: Option<HindsightObservationScopes>,
    transcript: CodexTranscript,
) -> Result<HindsightRetainRequest, HindsightConsumerError> {
    let mut metadata = BTreeMap::from([
        ("source_id".to_owned(), source_id.clone()),
        (
            "record_id".to_owned(),
            delivery.record_id.as_uuid().to_string(),
        ),
        (
            "record_version".to_owned(),
            delivery.record_version.to_string(),
        ),
        (
            "event_id".to_owned(),
            delivery.event_id.as_uuid().to_string(),
        ),
        (
            "revision_algorithm".to_owned(),
            CanonicalRevision::ALGORITHM.to_owned(),
        ),
        ("revision_digest".to_owned(), revision.digest_hex()),
        (
            "revision_byte_length".to_owned(),
            revision.byte_length().to_string(),
        ),
        (
            "renderer_profile".to_owned(),
            render_profile.name().to_owned(),
        ),
        ("renderer_version".to_owned(), "2".to_owned()),
        (
            "render_source_messages".to_owned(),
            transcript.render_stats.source_messages.to_string(),
        ),
        (
            "render_included_messages".to_owned(),
            transcript.render_stats.included_messages.to_string(),
        ),
        (
            "render_filtered_synthetic_messages".to_owned(),
            transcript
                .render_stats
                .filtered_synthetic_messages
                .to_string(),
        ),
        (
            "render_stripped_memory_echoes".to_owned(),
            transcript.render_stats.stripped_memory_echoes.to_string(),
        ),
        (
            "render_tool_calls".to_owned(),
            transcript.render_stats.tool_calls.to_string(),
        ),
        (
            "render_tool_results".to_owned(),
            transcript.render_stats.tool_results.to_string(),
        ),
        (
            "render_truncated_tool_results".to_owned(),
            transcript.render_stats.truncated_tool_results.to_string(),
        ),
    ]);
    if let Some(event_sequence) = delivery.event_sequence {
        metadata.insert("event_sequence".to_owned(), event_sequence.to_string());
    }
    if let Some(location) = &delivery.location {
        metadata.insert("root_role".to_owned(), location.root_role().to_owned());
        metadata.insert(
            "source_relative_path".to_owned(),
            location.source_relative_path().to_owned(),
        );
    }
    if let Some(session_id) = &transcript.session_id {
        metadata.insert("session_id".to_owned(), session_id.clone());
    }
    if let Some(session_cwd) = &transcript.session_cwd {
        metadata.insert("session_cwd".to_owned(), session_cwd.clone());
    }
    let workspace = transcript
        .session_cwd
        .as_deref()
        .and_then(|cwd| Path::new(cwd).file_name())
        .and_then(|value| value.to_str())
        .map(workspace_tag)
        .unwrap_or_else(|| "unknown".to_owned());
    let mut context = match render_profile {
        HindsightRenderProfile::LegacyV1 => "Codex agent-session transcript".to_owned(),
        HindsightRenderProfile::ReferenceMessage | HindsightRenderProfile::ReferenceToolAware => {
            "Conversation between a human collaborator (User) and the Codex coding agent (Assistant).".to_owned()
        }
    };
    if let Some(session_cwd) = transcript.session_cwd.as_deref() {
        match render_profile {
            HindsightRenderProfile::LegacyV1 => context.push_str(" from workspace "),
            HindsightRenderProfile::ReferenceMessage
            | HindsightRenderProfile::ReferenceToolAware => context.push_str(" Workspace: "),
        }
        context.push_str(session_cwd);
    }
    Ok(HindsightRetainRequest {
        bank_id: bank_id.to_owned(),
        document_id: source_id.clone(),
        source_id,
        content: transcript.content,
        context,
        timestamp: transcript.last_timestamp,
        metadata,
        tags: vec!["agent:codex".to_owned(), format!("workspace:{workspace}")],
        observation_scopes,
    })
}

fn workspace_tag(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    normalized
        .trim_matches('-')
        .chars()
        .take(80)
        .collect::<String>()
}

fn validate_bank_id(bank_id: &str) -> Result<(), HindsightConsumerError> {
    if bank_id.is_empty()
        || bank_id.trim() != bank_id
        || !bank_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(HindsightConsumerError::InvalidConfiguration(
            "Hindsight bank ID must be non-empty, trimmed, and use only ASCII letters, digits, '-', '_', or '.'"
                .to_owned(),
        ));
    }
    Ok(())
}

fn receipt_outcome(delivery: &DeliveryLease) -> DeliveryOutcome {
    if delivery.revision.is_some() {
        DeliveryOutcome::Acknowledged
    } else {
        // This pilot deliberately has no destructive learned-memory policy.
        // A raw archive tombstone does not imply facts should be erased.
        DeliveryOutcome::IgnoredByPolicy
    }
}

fn write_receipt(destination: &Path, receipt: &[u8]) -> Result<(), HindsightConsumerError> {
    let parent = destination.parent().expect("receipt path has a parent");
    fs::create_dir_all(parent)?;
    let filename = destination
        .file_name()
        .and_then(|value| value.to_str())
        .expect("receipt path has a UTF-8 filename");
    let temporary = parent.join(format!(".{filename}.{}.partial", Uuid::new_v4()));
    let result = (|| -> Result<(), HindsightConsumerError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(receipt)?;
        file.sync_all()?;
        fs::rename(&temporary, destination)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn receipt_bytes(
    delivery: &DeliveryLease,
    payload: Option<CanonicalRevision>,
    source_id: &str,
    bank_id: &str,
    render_profile: HindsightRenderProfile,
    observation_scopes: Option<HindsightObservationScopes>,
    operation_id: Option<&str>,
) -> Vec<u8> {
    let location = delivery.location.as_ref().map(|location| {
        json!({
            "root_role": location.root_role(),
            "source_relative_path": location.source_relative_path(),
        })
    });
    let payload = payload.map(|value| {
        json!({
            "algorithm": CanonicalRevision::ALGORITHM,
            "byte_length": value.byte_length(),
            "digest": value.digest_hex(),
        })
    });
    let mut receipt = json!({
        "format_version": 1,
        "consumer": "hindsight",
        "subscription_id": delivery.subscription_id.as_uuid().to_string(),
        "event_id": delivery.event_id.as_uuid().to_string(),
        "event_idempotency_key": format!("{}:{}", delivery.subscription_id, delivery.event_id.as_uuid()),
        "event_sequence": delivery.event_sequence,
        "record_id": delivery.record_id.as_uuid().to_string(),
        "record_version": delivery.record_version,
        "event_kind": event_kind_name(delivery.kind),
        "source_id": source_id,
        "document_id": source_id,
        "bank_id": bank_id,
        "renderer_profile": render_profile.name(),
        "operation_id": operation_id,
        "location": location,
        "payload": payload,
    });
    if let Some(observation_scopes) = observation_scopes {
        receipt["observation_scopes"] = Value::String(observation_scopes.name().to_owned());
    }
    let mut output = serde_json::to_vec(&receipt).expect("JSON values serialize without error");
    output.push(b'\n');
    output
}

fn receipt_matches(existing: &[u8], expected: &[u8]) -> Result<bool, HindsightConsumerError> {
    let mut existing: Value = serde_json::from_slice(existing).map_err(|error| {
        HindsightConsumerError::InvalidReceipt(format!(
            "existing receipt is not valid JSON: {error}"
        ))
    })?;
    let mut expected: Value =
        serde_json::from_slice(expected).expect("generated receipt is valid JSON");
    existing
        .as_object_mut()
        .expect("receipt JSON is an object")
        .remove("operation_id");
    expected
        .as_object_mut()
        .expect("receipt JSON is an object")
        .remove("operation_id");
    Ok(existing == expected)
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::RevisionCommitted => "revision_committed",
        EventKind::LocationChanged => "location_changed",
        EventKind::RecordTombstoned => "record_tombstoned",
        EventKind::RecordRestored => "record_restored",
    }
}

#[derive(Debug, Error)]
pub enum HindsightConsumerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Domain(#[from] crate::domain::DomainError),
    #[error("invalid Hindsight consumer configuration: {0}")]
    InvalidConfiguration(String),
    #[error("leased payload does not match its declared revision")]
    ProvenanceMismatch,
    #[error("byte-bearing delivery has no materialized payload")]
    MissingPayload,
    #[error("metadata-only delivery unexpectedly has a payload")]
    UnexpectedPayload,
    #[error("Codex transcript cannot be safely rendered for Hindsight: {0}")]
    InvalidTranscript(String),
    #[error("existing Hindsight receipt is malformed: {0}")]
    InvalidReceipt(String),
    #[error("existing Hindsight receipt conflicts with delivery provenance: {0}")]
    IdempotencyConflict(PathBuf),
    #[error("Hindsight runner failed: {0}")]
    Runner(String),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        HindsightConsumer, HindsightObservationScopes, HindsightRenderProfile,
        HindsightRetainRequest, HindsightRetainResponse, HindsightRunner, parse_codex_transcript,
    };
    use crate::catalog::Catalog;
    use crate::controller::{ControlledRunLimits, DeliveryRoots, run_path_consumer};
    use crate::delivery::{RetryPolicy, SubscriptionConfig, SubscriptionMode};
    use crate::domain::{CanonicalRevision, LogicalLocation, ProducerId, RecordIdentity};
    use crate::payload::{MaterializationCache, MaterializationLimits};

    #[derive(Default)]
    struct RecordingRunner {
        requests: Vec<HindsightRetainRequest>,
    }

    impl HindsightRunner for RecordingRunner {
        fn retain(
            &mut self,
            request: &HindsightRetainRequest,
        ) -> Result<HindsightRetainResponse, String> {
            self.requests.push(request.clone());
            Ok(HindsightRetainResponse {
                operation_id: Some("operation-1".to_owned()),
            })
        }
    }

    fn identity() -> RecordIdentity {
        RecordIdentity::new(
            ProducerId::new(),
            "codex",
            "session-a",
            "transcript",
            "primary",
        )
        .unwrap()
    }

    #[test]
    fn adapter_renders_only_messages_and_writes_a_provenance_receipt() {
        let directory = tempdir().unwrap();
        let source_root = directory.path().join("source");
        let cache_root = directory.path().join("cache");
        let receipt_root = directory.path().join("receipts");
        let source_path = source_root.join("sessions/a.jsonl");
        let bytes = concat!(
            "{\"timestamp\":\"2026-08-01T01:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session-a\",\"cwd\":\"/work/example\"}}\n",
            "{\"timestamp\":\"2026-08-01T01:01:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"Keep raw JSONL canonical.\"}}\n",
            "{\"timestamp\":\"2026-08-01T01:01:30Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"custom_tool_call\",\"message\":\"must not send\"}}\n",
            "{\"timestamp\":\"2026-08-01T01:02:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"I will use an adapter.\"}}\n"
        )
        .as_bytes();
        fs::create_dir_all(source_path.parent().unwrap()).unwrap();
        fs::write(&source_path, bytes).unwrap();
        let revision = CanonicalRevision::from_bytes(bytes);
        let mut catalog = Catalog::open_in_memory().unwrap();
        let subscription = catalog
            .create_subscription(
                SubscriptionConfig::new("hindsight", 1, true, false)
                    .unwrap()
                    .with_retry_policy(RetryPolicy::new(8, 0, 0).unwrap()),
                SubscriptionMode::ReplayEvents,
                0,
            )
            .unwrap();
        catalog
            .observe_present_at(
                &identity(),
                &LogicalLocation::new("active", "sessions/a.jsonl").unwrap(),
                revision,
                10,
            )
            .unwrap();
        let roots = DeliveryRoots::new(vec![("active".to_owned(), source_root)]).unwrap();
        let cache = MaterializationCache::new(
            &cache_root,
            MaterializationLimits::new(4096, 1, 4096).unwrap(),
        )
        .unwrap();
        let runner = RecordingRunner::default();
        let mut consumer = HindsightConsumer::new_with_render_profile_and_observation_scopes(
            &receipt_root,
            "codex-pilot",
            HindsightRenderProfile::LegacyV1,
            Some(HindsightObservationScopes::Shared),
            runner,
        )
        .unwrap();

        let report = run_path_consumer(
            &mut catalog,
            subscription.id,
            &roots,
            &cache,
            &mut consumer,
            ControlledRunLimits::new(1, 4096, 100).unwrap(),
            20,
        )
        .unwrap();

        assert_eq!(report.attempts.len(), 1);
        assert_eq!(consumer.runner.requests.len(), 1);
        let request = &consumer.runner.requests[0];
        assert!(
            request
                .content
                .contains("User (2026-08-01T01:01:00Z): Keep raw JSONL canonical.")
        );
        assert!(
            request
                .content
                .contains("Assistant (2026-08-01T01:02:00Z): I will use an adapter.")
        );
        assert!(!request.content.contains("must not send"));
        assert_eq!(request.timestamp.as_deref(), Some("2026-08-01T01:02:00Z"));
        assert_eq!(
            request.metadata.get("session_id").map(String::as_str),
            Some("session-a")
        );
        assert_eq!(request.tags, vec!["agent:codex", "workspace:example"]);
        assert_eq!(
            request.observation_scopes,
            Some(HindsightObservationScopes::Shared)
        );
        let subscription_root = fs::read_dir(&receipt_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let receipt = fs::read_to_string(
            fs::read_dir(subscription_root)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap();
        assert!(receipt.contains("\"consumer\":\"hindsight\""));
        assert!(receipt.contains("\"operation_id\":\"operation-1\""));
        assert!(receipt.contains("\"observation_scopes\":\"shared\""));
        assert!(receipt.contains(&revision.digest_hex()));
    }

    #[test]
    fn reference_message_profile_uses_final_response_items_and_filters_feedback() {
        let directory = tempdir().unwrap();
        let transcript_path = directory.path().join("reference.jsonl");
        fs::write(
            &transcript_path,
            concat!(
                "{\"timestamp\":\"2026-08-02T01:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session-reference\",\"cwd\":\"/work/example\"}}\n",
                "{\"timestamp\":\"2026-08-02T01:01:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"# AGENTS.md instructions for /work/example\\n<INSTRUCTIONS>ignore this</INSTRUCTIONS>\"}]}}\n",
                "{\"timestamp\":\"2026-08-02T01:02:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"<hindsight_memories>old fact</hindsight_memories> Keep raw JSONL canonical.\"}]}}\n",
                "{\"timestamp\":\"2026-08-02T01:03:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":[{\"type\":\"output_text\",\"text\":\"Do not retain commentary.\"}]}}\n",
                "{\"timestamp\":\"2026-08-02T01:04:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"I will retain only canonical source context.\"}]}}\n"
            ),
        )
        .unwrap();

        let transcript =
            parse_codex_transcript(&transcript_path, HindsightRenderProfile::ReferenceMessage)
                .unwrap();
        let content: serde_json::Value = serde_json::from_str(&transcript.content).unwrap();
        let messages = content.as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["timestamp"], "2026-08-02T01:02:00Z");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["timestamp"], "2026-08-02T01:04:00Z");
        assert!(transcript.content.contains("Keep raw JSONL canonical."));
        assert!(
            transcript
                .content
                .contains("I will retain only canonical source context.")
        );
        assert!(!transcript.content.contains("AGENTS.md instructions"));
        assert!(!transcript.content.contains("old fact"));
        assert!(!transcript.content.contains("Do not retain commentary."));
        assert_eq!(transcript.render_stats.filtered_synthetic_messages, 1);
        assert_eq!(transcript.render_stats.stripped_memory_echoes, 1);
    }

    #[test]
    fn reference_tool_profile_bounds_tool_output_and_groups_it_with_the_assistant() {
        let directory = tempdir().unwrap();
        let transcript_path = directory.path().join("tool-aware.jsonl");
        let oversized_output = "x".repeat(2_100);
        let transcript = concat!(
                "{\"timestamp\":\"2026-08-02T02:00:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"What changed?\"}]}}\n",
                "{\"timestamp\":\"2026-08-02T02:01:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"local_shell_call\",\"action\":{\"command\":[\"git\",\"status\"]}}}\n",
                "{\"timestamp\":\"2026-08-02T02:02:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"output\":\"__OVERSIZED_OUTPUT__\"}}\n",
                "{\"timestamp\":\"2026-08-02T02:03:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"The renderer is now profile-driven.\"}]}}\n"
            )
            .replace("__OVERSIZED_OUTPUT__", &oversized_output);
        fs::write(&transcript_path, transcript).unwrap();

        let transcript =
            parse_codex_transcript(&transcript_path, HindsightRenderProfile::ReferenceToolAware)
                .unwrap();
        let content: serde_json::Value = serde_json::from_str(&transcript.content).unwrap();
        let messages = content.as_array().unwrap();
        assert_eq!(messages.len(), 2);
        let assistant_blocks = messages[1]["content"].as_array().unwrap();
        assert_eq!(assistant_blocks[0]["type"], "tool_use");
        assert_eq!(assistant_blocks[0]["name"], "shell");
        assert_eq!(assistant_blocks[1]["type"], "tool_result");
        assert!(
            assistant_blocks[1]["content"]
                .as_str()
                .unwrap()
                .ends_with("... (truncated)")
        );
        assert_eq!(assistant_blocks[2]["type"], "text");
        assert_eq!(transcript.render_stats.tool_calls, 1);
        assert_eq!(transcript.render_stats.tool_results, 1);
        assert_eq!(transcript.render_stats.truncated_tool_results, 1);
    }
}
