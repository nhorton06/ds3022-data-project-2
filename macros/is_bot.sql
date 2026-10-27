{#
    Bot rule, applied to PR authors, reviewers, and commit authors:
    a GitHub user is a bot if user.type = 'Bot' OR user.login ends in '[bot]'.
    A missing user (e.g. a commit email not linked to an account) is not a bot.
    `user` is a JSON expression for the user object, e.g. payload->'user'.
#}
{% macro is_bot(user) %}
    coalesce(({{ user }}->>'type') = 'Bot' or ({{ user }}->>'login') like '%[bot]', false)
{% endmacro %}
