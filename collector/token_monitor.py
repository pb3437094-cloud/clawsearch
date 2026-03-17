import os
import asyncio
from dotenv import load_dotenv
from axiomtradeapi import AxiomTradeClient

load_dotenv()

async def main():
    client = AxiomTradeClient(
        username=os.getenv("EMAIL_ADDRESS"),
        password=os.getenv("AXIOM_PASSWORD")
    )

    if not client.is_authenticated():
        print("Logging in...")
        client.login()

    print("Authenticated as:", client.auth_manager.username)

    client.connect()
    print("Connected to session.")

    async def handle_new_tokens(tokens):
        for token in tokens:
            print("🚀 New Token Detected:")
            print(token)
            print("-" * 50)

    await client.subscribe_new_tokens(handle_new_tokens)
    await client.ws.start()

if __name__ == "__main__":
    asyncio.run(main())
