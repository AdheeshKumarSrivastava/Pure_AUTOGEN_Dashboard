from __future__ import annotations

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.teams import RoundRobinGroupChat

from llm import make_model_client
from prompts import SYSTEM_MESSAGES
from models import AnalysisPlan, SQLBuildOutput


def build_team():
    # Structured output clients
    planner_client = make_model_client(response_format=AnalysisPlan)
    sql_client = make_model_client(response_format=SQLBuildOutput)

    # Default client (NO response_format)
    default_client = make_model_client()

    planner = AssistantAgent(name="planner", model_client=planner_client, system_message=SYSTEM_MESSAGES["planner"])
    schema_profiler = AssistantAgent(name="schema_profiler", model_client=default_client, system_message=SYSTEM_MESSAGES["schema_profiler"])
    table_describer = AssistantAgent(name="table_describer", model_client=default_client, system_message=SYSTEM_MESSAGES["table_describer"])
    sql_builder = AssistantAgent(name="sql_builder", model_client=sql_client, system_message=SYSTEM_MESSAGES["sql_builder"])
    python_analyst = AssistantAgent(name="python_analyst", model_client=default_client, system_message=SYSTEM_MESSAGES["python_analyst"])
    dashboard_builder = AssistantAgent(name="dashboard_builder", model_client=default_client, system_message=SYSTEM_MESSAGES["dashboard_builder"])
    reviewer = AssistantAgent(name="reviewer", model_client=default_client, system_message=SYSTEM_MESSAGES["reviewer"])

    # ✅ Termination condition (different autogen versions have slightly different APIs)
    termination_condition = None
    try:
        from autogen_agentchat.conditions import TextMentionTermination, MaxMessageTermination

        # Stop when reviewer says APPROVE_STEP
        termination_condition = TextMentionTermination("APPROVE_STEP") | MaxMessageTermination(20)
    except Exception:
        # Fallback: no conditions module in this version
        termination_condition = None

    if termination_condition is not None:
        team = RoundRobinGroupChat(
            participants=[
                planner,
                schema_profiler,
                table_describer,
                sql_builder,
                python_analyst,
                dashboard_builder,
                reviewer,
            ],
            termination_condition=termination_condition,
        )
    else:
        # Fallback: still works, but may not auto-stop in older versions
        team = RoundRobinGroupChat(
            participants=[
                planner,
                schema_profiler,
                table_describer,
                sql_builder,
                python_analyst,
                dashboard_builder,
                reviewer,
            ]
        )

    return team, [planner_client, sql_client, default_client]