use pretty_assertions::assert_eq;

use super::*;

#[tokio::test]
async fn prompt_hook_uses_default_model_when_config_model_is_unset() {
    let captured_request = std::sync::Arc::new(std::sync::Mutex::new(None));
    let runner = PromptHookRunner::new({
        let captured_request = std::sync::Arc::clone(&captured_request);
        move |request| {
            let captured_request = std::sync::Arc::clone(&captured_request);
            async move {
                *captured_request.lock().expect("captured request lock") = Some(request);
                Ok(r#"{"ok":true}"#.to_string())
            }
        }
    });
    let handler = prompt_handler(/*model*/ None);

    let result = run_prompt(
        &runner,
        &handler,
        r#"{"hook_event_name":"Stop"}"#,
        "gpt-thread".to_string(),
    )
    .await;

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.error, None);
    assert_eq!(
        captured_request
            .lock()
            .expect("captured request lock")
            .clone()
            .expect("prompt request"),
        PromptHookRequest {
            event_name: HookEventName::Stop,
            prompt: "Check: {\"hook_event_name\":\"Stop\"}".to_string(),
            model: "gpt-thread".to_string(),
        }
    );
}

#[test]
fn render_prompt_replaces_arguments_placeholder() {
    assert_eq!(
        render_prompt("Check: $ARGUMENTS", r#"{"event":"Stop"}"#),
        r#"Check: {"event":"Stop"}"#
    );
}

#[test]
fn render_prompt_appends_arguments_without_placeholder() {
    assert_eq!(
        render_prompt("Check the turn.", r#"{"event":"Stop"}"#),
        "Check the turn.\n\n{\"event\":\"Stop\"}"
    );
}

#[test]
fn stop_ok_false_becomes_block_decision() {
    assert_json_eq(
        prompt_output_to_command_stdout(
            HookEventName::Stop,
            /*continue_on_block*/ false,
            r#"{"ok":false,"reason":"mention tests"}"#,
        )
        .expect("prompt output"),
        json!({
            "decision": "block",
            "reason": "mention tests",
        }),
    );
}

#[test]
fn permission_request_ok_false_records_reason_without_decision() {
    assert_json_eq(
        prompt_output_to_command_stdout(
            HookEventName::PermissionRequest,
            /*continue_on_block*/ false,
            r#"{"ok":false,"reason":"looks suspicious"}"#,
        )
        .expect("prompt output"),
        json!({
            "systemMessage": "looks suspicious",
        }),
    );
}

#[test]
fn post_tool_use_ok_false_honors_continue_on_block() {
    assert_json_eq(
        prompt_output_to_command_stdout(
            HookEventName::PostToolUse,
            /*continue_on_block*/ true,
            r#"{"ok":false,"reason":"summarize the command output"}"#,
        )
        .expect("prompt output"),
        json!({
            "decision": "block",
            "reason": "summarize the command output",
        }),
    );
    assert_json_eq(
        prompt_output_to_command_stdout(
            HookEventName::PostToolUse,
            /*continue_on_block*/ false,
            r#"{"ok":false,"reason":"stop here"}"#,
        )
        .expect("prompt output"),
        json!({
            "continue": false,
            "decision": "block",
            "reason": "stop here",
            "stopReason": "stop here",
        }),
    );
}

#[test]
fn every_event_declares_prompt_behavior() {
    for event_name in [
        HookEventName::PreToolUse,
        HookEventName::PermissionRequest,
        HookEventName::PostToolUse,
        HookEventName::PreCompact,
        HookEventName::PostCompact,
        HookEventName::SessionStart,
        HookEventName::UserPromptSubmit,
        HookEventName::SubagentStart,
        HookEventName::SubagentStop,
        HookEventName::Stop,
    ] {
        let _ = prompt_hook_behavior(event_name);
    }
}

fn assert_json_eq(actual: String, expected: serde_json::Value) {
    let actual: serde_json::Value = serde_json::from_str(&actual).expect("json output");
    assert_eq!(actual, expected);
}

fn prompt_handler(model: Option<String>) -> ConfiguredHandler {
    ConfiguredHandler {
        event_name: HookEventName::Stop,
        matcher: None,
        kind: ConfiguredHandlerKind::Prompt {
            prompt: "Check: $ARGUMENTS".to_string(),
            model,
            timeout_sec: 30,
            continue_on_block: true,
        },
        status_message: None,
        source_path: codex_utils_absolute_path::AbsolutePathBuf::current_dir().expect("cwd"),
        source: codex_protocol::protocol::HookSource::User,
        display_order: 0,
        env: std::collections::HashMap::new(),
    }
}
