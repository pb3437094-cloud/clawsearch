from axiomtradeapi import AxiomTradeClient
import json
import os
from dotenv import load_dotenv

load_dotenv()

email = os.getenv("EMAIL_ADDRESS")
password = os.getenv("AXIOM_PASSWORD")

print("Creating client...")
client = AxiomTradeClient(username=email, password=password)

print("Logging in...")
client.login()

print("Authenticated:", client.is_authenticated)

print("\n--- TESTING ENDPOINTS ---\n")

# 1 Trending tokens
try:
    trending = client.get_trending_tokens()
    print("Trending tokens type:", type(trending))
    print("Trending sample:")
    print(json.dumps(trending, indent=2)[:800])
except Exception as e:
    print("Trending tokens failed:", e)


# 2 Token discovery
try:
    tokens = client.get_tokens()
    print("\nToken discovery type:", type(tokens))
    print("Token sample:")
    print(json.dumps(tokens, indent=2)[:800])
except Exception as e:
    print("Token discovery failed:", e)


# 3 Active users
try:
    users = client.get_active_axiom_users()
    print("\nActive users:")
    print(users)
except Exception as e:
    print("Active users failed:", e)


# 4 Token analysis (requires address)
try:
    if trending:
        token_address = None

        if isinstance(trending, list):
            token_address = trending[0].get("tokenAddress")

        if isinstance(trending, dict):
            first = list(trending.values())[0]
            token_address = first.get("tokenAddress")

        if token_address:
            analysis = client.get_token_analysis(token_address)

            print("\nToken analysis sample:")
            print(json.dumps(analysis, indent=2)[:800])
except Exception as e:
    print("Token analysis failed:", e)
