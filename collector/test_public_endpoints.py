from axiomtradeapi import AxiomTradeClient

client = AxiomTradeClient()

print("Testing public endpoints...")

try:
    tokens = client.get_tokens()

    print("SUCCESS")
    print(type(tokens))
    print(tokens)

except Exception as e:
    print("FAILED")
    print(e)
