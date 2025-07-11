import asyncio
import os

from agents import Agent, OpenAIChatCompletionsModel, Runner
from agents.mcp import MCPServerStreamableHttp
from openai import AsyncOpenAI


MAIN_MODEL = "gpt-4o"
api_key=os.getenv('OPENAI_API_KEY', 'your api key')
base_url = 'https://api.openai.com/v1/'
main_client = AsyncOpenAI(
    api_key=api_key,
    base_url=base_url,
)
model = OpenAIChatCompletionsModel(
    model=MAIN_MODEL, openai_client=main_client
)
INSTRUCTIONS = ''

async def main():
    # mcp server config
    async with MCPServerStreamableHttp(
        name="test mcp server",
        params={"url": "http://127.0.0.1:8000/mcp"},
        client_session_timeout_seconds=120,
    ) as server:
        # use openai-agent sdk as client and agent
        mcp_agent = Agent(
            name="MCPAgent",
            instructions=INSTRUCTIONS,
            model=model,
            mcp_servers=[server]
        )
        query = "what mcp tools do you have"
        res = await Runner.run(mcp_agent, query)
        print(res.final_output)


if __name__ == "__main__":
    asyncio.run(main())
