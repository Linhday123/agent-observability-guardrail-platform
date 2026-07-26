-- Gold Warehouse indexes for common analytical queries.
-- These indexes support DP3 when filtering actions by agent and time.

CREATE INDEX idx_fact_agent_action_agent_key
    ON fact_agent_action(agent_key);

CREATE INDEX idx_fact_agent_action_called_at
    ON fact_agent_action(called_at);
