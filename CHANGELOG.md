# Changelog

## [0.4.0](https://github.com/chuse-ai/agent-batch/compare/v0.3.0...v0.4.0) (2026-10-27)


### ⚠ BREAKING CHANGES

* `POST /jobs` is replaced by `POST /batches` and `POST /batches/{id}/jobs`; `prompt` is now `input`, and a job's Result contract and limits come from its Batch's agent. The Runner protocol's job content carries `instructions` and `input` in place of `prompt`, so Runners and hubs must be upgraded together.

### Features

* run every job in a Batch that shares its agent, Canary and breaker; label and list jobs ([#33](https://github.com/chuse-ai/agent-batch/issues/33)) ([108b54f](https://github.com/chuse-ai/agent-batch/commit/108b54fb82d038a08b1d5094b7a34702330b9c1b))
* **runner:** call the model provider at RUNNER_MODEL_BASE_URL ([#32](https://github.com/chuse-ai/agent-batch/issues/32)) ([72e83f0](https://github.com/chuse-ai/agent-batch/commit/72e83f0ca929b3925c340795443e997375291a1f))


### Bug Fixes

* **contract:** leave the Runner's own calls out of the public API ([#31](https://github.com/chuse-ai/agent-batch/issues/31)) ([af8714f](https://github.com/chuse-ai/agent-batch/commit/af8714f4f690c205936fb4ca26acd88a2a4a56dd))
* **deps:** upgrade pi to 1.4.0, whose journals older Runners cannot open ([#39](https://github.com/chuse-ai/agent-batch/issues/39)) ([4218547](https://github.com/chuse-ai/agent-batch/commit/42185475da7da3632773229d3bddbff921ff05c4))
* run the quickstart from the latest release's Compose file and images ([#30](https://github.com/chuse-ai/agent-batch/issues/30)) ([70cf1a4](https://github.com/chuse-ai/agent-batch/commit/70cf1a49b9eb0dfbab84726b5c78a6d5c0029753))


### Performance Improvements

* **runner:** give sandbox-runtime the shell's path, not its name ([#38](https://github.com/chuse-ai/agent-batch/issues/38)) ([823db6d](https://github.com/chuse-ai/agent-batch/commit/823db6d38da50b497573aca17aa937da970ac101))

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
