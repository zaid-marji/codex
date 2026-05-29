use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::InternalSessionSource as CoreInternalSessionSource;
use codex_protocol::protocol::SessionSource as CoreSessionSource;
use codex_protocol::protocol::SubAgentSource as CoreSubAgentSource;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, TS, Default)]
#[serde(rename_all = "lowercase")]
#[ts(rename_all = "lowercase")]
pub enum SessionSource {
    Cli,
    #[default]
    VSCode,
    Exec,
    Mcp,
    Custom(String),
    Internal(InternalSessionSource),
    SubAgent(SubAgentSource),
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum InternalSessionSource {
    MemoryConsolidation,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum SubAgentSource {
    Review,
    Compact,
    ThreadSpawn {
        parent_thread_id: ThreadId,
        depth: i32,
        #[serde(default)]
        agent_path: Option<AgentPath>,
        #[serde(default)]
        agent_nickname: Option<String>,
        #[serde(default, alias = "agent_type")]
        agent_role: Option<String>,
    },
    MemoryConsolidation,
    Other(String),
}

impl From<CoreSessionSource> for SessionSource {
    fn from(value: CoreSessionSource) -> Self {
        match value {
            CoreSessionSource::Cli => SessionSource::Cli,
            CoreSessionSource::VSCode => SessionSource::VSCode,
            CoreSessionSource::Exec => SessionSource::Exec,
            CoreSessionSource::Mcp => SessionSource::Mcp,
            CoreSessionSource::Custom(source) => SessionSource::Custom(source),
            CoreSessionSource::Internal(source) => SessionSource::Internal(source.into()),
            CoreSessionSource::SubAgent(source) => SessionSource::SubAgent(source.into()),
            CoreSessionSource::Unknown => SessionSource::Unknown,
        }
    }
}

impl From<SessionSource> for CoreSessionSource {
    fn from(value: SessionSource) -> Self {
        match value {
            SessionSource::Cli => CoreSessionSource::Cli,
            SessionSource::VSCode => CoreSessionSource::VSCode,
            SessionSource::Exec => CoreSessionSource::Exec,
            SessionSource::Mcp => CoreSessionSource::Mcp,
            SessionSource::Custom(source) => CoreSessionSource::Custom(source),
            SessionSource::Internal(source) => CoreSessionSource::Internal(source.into()),
            SessionSource::SubAgent(source) => CoreSessionSource::SubAgent(source.into()),
            SessionSource::Unknown => CoreSessionSource::Unknown,
        }
    }
}

impl From<CoreInternalSessionSource> for InternalSessionSource {
    fn from(value: CoreInternalSessionSource) -> Self {
        match value {
            CoreInternalSessionSource::MemoryConsolidation => {
                InternalSessionSource::MemoryConsolidation
            }
        }
    }
}

impl From<InternalSessionSource> for CoreInternalSessionSource {
    fn from(value: InternalSessionSource) -> Self {
        match value {
            InternalSessionSource::MemoryConsolidation => {
                CoreInternalSessionSource::MemoryConsolidation
            }
        }
    }
}

impl From<CoreSubAgentSource> for SubAgentSource {
    fn from(value: CoreSubAgentSource) -> Self {
        match value {
            CoreSubAgentSource::Review => SubAgentSource::Review,
            CoreSubAgentSource::Compact => SubAgentSource::Compact,
            CoreSubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_nickname,
                agent_role,
            } => SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_nickname,
                agent_role,
            },
            CoreSubAgentSource::MemoryConsolidation => SubAgentSource::MemoryConsolidation,
            CoreSubAgentSource::Other(label) => SubAgentSource::Other(label),
        }
    }
}

impl From<SubAgentSource> for CoreSubAgentSource {
    fn from(value: SubAgentSource) -> Self {
        match value {
            SubAgentSource::Review => CoreSubAgentSource::Review,
            SubAgentSource::Compact => CoreSubAgentSource::Compact,
            SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_nickname,
                agent_role,
            } => CoreSubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth,
                agent_path,
                agent_nickname,
                agent_role,
            },
            SubAgentSource::MemoryConsolidation => CoreSubAgentSource::MemoryConsolidation,
            SubAgentSource::Other(label) => CoreSubAgentSource::Other(label),
        }
    }
}
