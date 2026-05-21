use crate::CloudConfigFragment;
use crate::ConfigLayerEntry;
use crate::RequirementSource;
use crate::RequirementsLayer;
use crate::cloud_config_layers::CloudConfigLayerError;
use crate::cloud_config_layers::cloud_config_layers_from_fragments_strict;
use crate::cloud_config_layers_from_fragments;
use codex_utils_absolute_path::AbsolutePathBuf;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::future::Shared;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::future::Future;
use thiserror::Error;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CloudConfigBundle {
    pub config_toml: CloudConfigTomlBundle,
    pub requirements_toml: CloudRequirementsTomlBundle,
}

impl CloudConfigBundle {
    pub fn is_empty(&self) -> bool {
        let CloudConfigBundle {
            config_toml,
            requirements_toml,
        } = self;
        let CloudConfigTomlBundle {
            enterprise_managed: config_enterprise_managed,
        } = config_toml;
        let CloudRequirementsTomlBundle {
            enterprise_managed: requirements_enterprise_managed,
        } = requirements_toml;

        config_enterprise_managed.is_empty() && requirements_enterprise_managed.is_empty()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CloudConfigTomlBundle {
    pub enterprise_managed: Vec<CloudConfigFragment>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CloudRequirementsTomlBundle {
    pub enterprise_managed: Vec<CloudRequirementsFragment>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CloudRequirementsFragment {
    pub id: String,
    pub name: String,
    pub contents: String,
}

/// Cloud config bundle converted into semantic layer buckets.
///
/// This is not a final config stack. Callers still decide where each bucket is
/// inserted relative to local/system/user layers.
#[derive(Clone, Debug)]
pub struct CloudConfigBundleLayers {
    /// Enterprise-managed config layers in `ConfigLayerStack` order.
    pub enterprise_managed_config: Vec<ConfigLayerEntry>,
    /// Enterprise-managed requirements layers in requirements composition order.
    pub enterprise_managed_requirements: Vec<RequirementsLayer>,
}

impl CloudConfigBundleLayers {
    pub fn from_bundle(
        bundle: CloudConfigBundle,
        base_dir: &AbsolutePathBuf,
    ) -> Result<Self, CloudConfigLayerError> {
        Self::from_bundle_impl(bundle, base_dir, /*strict_config*/ false)
    }

    pub fn from_bundle_strict_config(
        bundle: CloudConfigBundle,
        base_dir: &AbsolutePathBuf,
    ) -> Result<Self, CloudConfigLayerError> {
        Self::from_bundle_impl(bundle, base_dir, /*strict_config*/ true)
    }

    fn from_bundle_impl(
        bundle: CloudConfigBundle,
        base_dir: &AbsolutePathBuf,
        strict_config: bool,
    ) -> Result<Self, CloudConfigLayerError> {
        // Keep this destructuring exhaustive so adding a new bundle bucket forces
        // an explicit choice about how it becomes layer data.
        let CloudConfigBundle {
            config_toml:
                CloudConfigTomlBundle {
                    enterprise_managed: config_enterprise_managed,
                },
            requirements_toml:
                CloudRequirementsTomlBundle {
                    enterprise_managed: requirements_enterprise_managed,
                },
        } = bundle;

        let enterprise_managed_config = if strict_config {
            cloud_config_layers_from_fragments_strict(config_enterprise_managed, base_dir)?
        } else {
            cloud_config_layers_from_fragments(config_enterprise_managed, base_dir)?
        };

        let mut enterprise_managed_requirements = requirements_enterprise_managed
            .into_iter()
            .map(|fragment| {
                RequirementsLayer::from_toml(
                    RequirementSource::EnterpriseManaged {
                        id: fragment.id,
                        name: fragment.name,
                    },
                    fragment.contents,
                )
                .with_base_dir(base_dir.clone())
            })
            .collect::<Vec<_>>();
        // Bundle fragments arrive highest-priority first, while requirements
        // composition folds lowest-priority to highest-priority.
        enterprise_managed_requirements.reverse();

        Ok(Self {
            enterprise_managed_config,
            enterprise_managed_requirements,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloudConfigBundleLoadErrorCode {
    Auth,
    Timeout,
    RequestFailed,
    InvalidBundle,
    Internal,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct CloudConfigBundleLoadError {
    code: CloudConfigBundleLoadErrorCode,
    message: String,
    status_code: Option<u16>,
}

impl CloudConfigBundleLoadError {
    pub fn new(
        code: CloudConfigBundleLoadErrorCode,
        status_code: Option<u16>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            status_code,
        }
    }

    pub fn code(&self) -> CloudConfigBundleLoadErrorCode {
        self.code
    }

    pub fn status_code(&self) -> Option<u16> {
        self.status_code
    }
}

#[derive(Clone)]
pub struct CloudConfigBundleLoader {
    fut: Shared<BoxFuture<'static, Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError>>>,
}

impl CloudConfigBundleLoader {
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Output = Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError>>
            + Send
            + 'static,
    {
        Self {
            fut: fut.boxed().shared(),
        }
    }

    pub async fn get(&self) -> Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError> {
        self.fut.clone().await
    }
}

impl fmt::Debug for CloudConfigBundleLoader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudConfigBundleLoader").finish()
    }
}

impl Default for CloudConfigBundleLoader {
    fn default() -> Self {
        Self::new(async { Ok(None) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConfigLayerSource;
    use crate::ConfigRequirementsToml;
    use crate::compose_requirements;
    use codex_protocol::protocol::AskForApproval;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tempfile::tempdir;

    #[tokio::test]
    async fn shared_future_runs_once() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let loader = CloudConfigBundleLoader::new(async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(Some(CloudConfigBundle::default()))
        });

        let (first, second) = tokio::join!(loader.get(), loader.get());
        assert_eq!(first, second);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn bundle_layers_preserve_enterprise_managed_bucket_order() {
        let tempdir = tempdir().expect("tempdir");
        let base_dir = AbsolutePathBuf::from_absolute_path(tempdir.path()).expect("absolute path");
        let layers = CloudConfigBundleLayers::from_bundle(
            CloudConfigBundle {
                config_toml: CloudConfigTomlBundle {
                    enterprise_managed: vec![
                        CloudConfigFragment {
                            id: "cfg_high".to_string(),
                            name: "High config".to_string(),
                            contents: "model = \"high\"".to_string(),
                        },
                        CloudConfigFragment {
                            id: "cfg_low".to_string(),
                            name: "Low config".to_string(),
                            contents: "model = \"low\"".to_string(),
                        },
                    ],
                },
                requirements_toml: CloudRequirementsTomlBundle {
                    enterprise_managed: vec![
                        CloudRequirementsFragment {
                            id: "req_high".to_string(),
                            name: "High requirements".to_string(),
                            contents: "allowed_approval_policies = [\"on-request\"]".to_string(),
                        },
                        CloudRequirementsFragment {
                            id: "req_low".to_string(),
                            name: "Low requirements".to_string(),
                            contents: "allowed_approval_policies = [\"never\"]".to_string(),
                        },
                    ],
                },
            },
            &base_dir,
        )
        .expect("bundle should be converted into layers");

        assert_eq!(
            layers
                .enterprise_managed_config
                .iter()
                .map(|layer| layer.name.clone())
                .collect::<Vec<_>>(),
            vec![
                ConfigLayerSource::EnterpriseManaged {
                    id: "cfg_low".to_string(),
                    name: "Low config".to_string(),
                },
                ConfigLayerSource::EnterpriseManaged {
                    id: "cfg_high".to_string(),
                    name: "High config".to_string(),
                },
            ]
        );
        assert_eq!(
            compose_requirements(layers.enterprise_managed_requirements)
                .expect("requirements should compose")
                .expect("requirements should be present")
                .into_toml(),
            ConfigRequirementsToml {
                allowed_approval_policies: Some(vec![AskForApproval::OnRequest]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn bundle_layers_can_strict_validate_enterprise_managed_config() {
        let tempdir = tempdir().expect("tempdir");
        let base_dir = AbsolutePathBuf::from_absolute_path(tempdir.path()).expect("absolute path");
        let err = CloudConfigBundleLayers::from_bundle_strict_config(
            CloudConfigBundle {
                config_toml: CloudConfigTomlBundle {
                    enterprise_managed: vec![CloudConfigFragment {
                        id: "cfg".to_string(),
                        name: "Cloud config".to_string(),
                        contents: "unknown_key = true".to_string(),
                    }],
                },
                requirements_toml: CloudRequirementsTomlBundle {
                    enterprise_managed: Vec::new(),
                },
            },
            &base_dir,
        )
        .expect_err("strict config should reject unknown fields");

        assert_eq!(
            err,
            CloudConfigLayerError::Invalid {
                fragment: crate::CloudConfigFragmentSource {
                    id: "cfg".to_string(),
                    name: "Cloud config".to_string(),
                },
                message: "unknown configuration field `unknown_key`".to_string(),
            }
        );
    }
}
