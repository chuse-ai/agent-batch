export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    // A warning, not an error: the merge queue lints the squash commit, whose footer holds lines
    // GitHub writes, such as `Co-authored-by:` trailers and, after a `BREAKING CHANGE:`, the
    // squashed commits' titles with a `* ` before them. Neither the hook nor CI runs `--strict`.
    "footer-max-line-length": [1, "always", 100],
  },
};
