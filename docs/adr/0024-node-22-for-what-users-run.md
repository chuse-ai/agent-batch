# What users run supports Node 22; the hub runs on runtimes we choose

The `agent-batch` CLI and the separately run Runner, each bundling the shared contract, runs on whatever Node its user installed, so it supports every Node release line still maintained: Node 22 onwards, maintained until 2027-04-30. The tools it builds on accept Node 22 too: wrangler needs 22.0, Vite+ 22.18 and Pi 22.19, so a higher floor would turn away users whom every dependency accepts. `engines.node` names the lowest Node 22 release these dependencies accept: `>=22.19.0`. When an update of one of them needs a later Node 22 release, the floor rises to it: users keep a supported line current within itself, so the floor tracks the line, not the oldest release in it.

The hub runs only on runtimes we choose: its Docker image's Node (`engines.node` `>=26` in the hub's package) and current workerd. So the hub may use any JavaScript they implement, and the code users run may use only what Node 22 has.

TypeScript enforces the split. The Runner, the CLI and the contract compile against `lib: es2024`, the language level Node 22 implements, as the `@tsconfig/node22` base sets it, so they cannot call an API Node 22 lacks; the hub compiles against `esnext`. `@types/node` stays at the oldest supported line's major for the same reason, and Dependabot does not propose newer majors.

## Considered options

- Node 24 as the floor: its APIs for the CLI and Runner, but Node 22 users turned away while every dependency still accepts them.
- Holding a dependency at an older major to keep an older Node 22 release: it falls behind on fixes and needs a Dependabot ignore rule of its own, to spare users an update within a line they already run.
- `esnext` everywhere: code shared with the CLI could call APIs that users' Node lacks, and nothing would catch it before it fails on their machines.

## Consequences

When Node 22 reaches its end of life, the floor moves to Node 24 in one change: `engines.node`, the user-run packages' `lib`, and `@types/node`'s major. The contract is shared by the hub and the CLI, so it keeps to the users' floor even where the hub could do more.
