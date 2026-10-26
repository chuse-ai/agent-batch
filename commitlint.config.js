export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    // The queue's squash merge adds a `Co-authored-by:` trailer for a PR's other authors, which runs
    // past 100 characters for an app's bot address, as on every release PR. The limit is the preset's
    // house style, not Conventional Commits' (conventional-changelog/commitlint#4627).
    "footer-max-line-length": [0],
  },
};
