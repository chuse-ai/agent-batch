# Changelog

## [0.3.0](https://github.com/chuse-ai/agent-batch/compare/v0.2.0...v0.3.0) (2026-10-26)


### ⚠ BREAKING CHANGES

* send Input files apart from the sync ([#21](https://github.com/chuse-ai/agent-batch/issues/21))
* give a failed job its failure, with the cause's message ([#19](https://github.com/chuse-ai/agent-batch/issues/19))

### Features

* give a failed job its failure, with the cause's message ([#19](https://github.com/chuse-ai/agent-batch/issues/19)) ([02f2d1c](https://github.com/chuse-ai/agent-batch/commit/02f2d1cac187bfa19a3eff89a27e4675c6197966))
* **runner:** sign in to the model provider's subscription ([#17](https://github.com/chuse-ai/agent-batch/issues/17)) ([3c33013](https://github.com/chuse-ai/agent-batch/commit/3c330131545dffe5b4f1a32e6a1a1ddc37c3cdd4))


### Bug Fixes

* **contract:** refuse results nested too deeply ([#15](https://github.com/chuse-ai/agent-batch/issues/15)) ([11589a2](https://github.com/chuse-ai/agent-batch/commit/11589a2f2977ba81f66a971b78b6b6c7f9850a8c))
* **runner:** end a job whose tool call never ends ([#20](https://github.com/chuse-ai/agent-batch/issues/20)) ([76753d4](https://github.com/chuse-ai/agent-batch/commit/76753d4684989629a26f3bace6b1d84066bd18cc))
* **runner:** kill a job's process, not the Runner, when the host runs out of memory ([#22](https://github.com/chuse-ai/agent-batch/issues/22)) ([537949b](https://github.com/chuse-ai/agent-batch/commit/537949b2a9876f3d2d6e44aa079d67e8325b8de5))
* send Input files apart from the sync ([#21](https://github.com/chuse-ai/agent-batch/issues/21)) ([e51de33](https://github.com/chuse-ai/agent-batch/commit/e51de33c612d081f85493a5d0a55075c8009d917))
