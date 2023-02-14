import mysql.connector
import web3
import time
import datetime
import requests
import json
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
from eth_utils import encode_hex, function_abi_to_4byte_selector, add_0x_prefix
from web3._utils.contracts import encode_abi
from web3._utils.abi import get_abi_output_types
from web3.types import HexBytes
# Connect to the database
mydb = mysql.connector.connect(
  host="",
  user="",
  password="",
  database="wallet"
)

# Create a cursor for executing SQL commands
mycursor = mydb.cursor()


# Connect to Binance Smart Chain
provider = HTTPProvider('https://bsc-dataseed.binance.org/')
w3 = Web3(provider)
w3.provider.chain_id = 56


#w3 = web3.Web3(web3.HTTPProvider("https://bsc-dataseed.binance.org/"))
#w3 = Web3(Web3.HTTPProvider("https://bsc-dataseed.binance.org/"))

 # ABI for the smart contract
contract_abi_rate = '[{"inputs":[{"internalType":"contract MultiWrapper","name":"_multiWrapper","type":"address"},{"internalType":"contract IOracle[]","name":"existingOracles","type":"address[]"},{"internalType":"enum OffchainOracle.OracleType[]","name":"oracleTypes","type":"uint8[]"},{"internalType":"contract IERC20[]","name":"existingConnectors","type":"address[]"},{"internalType":"contract IERC20","name":"wBase","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"contract IERC20","name":"connector","type":"address"}],"name":"ConnectorAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"contract IERC20","name":"connector","type":"address"}],"name":"ConnectorRemoved","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"contract MultiWrapper","name":"multiWrapper","type":"address"}],"name":"MultiWrapperUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"contract IOracle","name":"oracle","type":"address"},{"indexed":false,"internalType":"enum OffchainOracle.OracleType","name":"oracleType","type":"uint8"}],"name":"OracleAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"contract IOracle","name":"oracle","type":"address"},{"indexed":false,"internalType":"enum OffchainOracle.OracleType","name":"oracleType","type":"uint8"}],"name":"OracleRemoved","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[{"internalType":"contract IERC20","name":"connector","type":"address"}],"name":"addConnector","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IOracle","name":"oracle","type":"address"},{"internalType":"enum OffchainOracle.OracleType","name":"oracleKind","type":"uint8"}],"name":"addOracle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"connectors","outputs":[{"internalType":"contract IERC20[]","name":"allConnectors","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20","name":"srcToken","type":"address"},{"internalType":"contract IERC20","name":"dstToken","type":"address"},{"internalType":"bool","name":"useWrappers","type":"bool"}],"name":"getRate","outputs":[{"internalType":"uint256","name":"weightedRate","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20","name":"srcToken","type":"address"},{"internalType":"bool","name":"useSrcWrappers","type":"bool"}],"name":"getRateToEth","outputs":[{"internalType":"uint256","name":"weightedRate","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"multiWrapper","outputs":[{"internalType":"contract MultiWrapper","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"oracles","outputs":[{"internalType":"contract IOracle[]","name":"allOracles","type":"address[]"},{"internalType":"enum OffchainOracle.OracleType[]","name":"oracleTypes","type":"uint8[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IERC20","name":"connector","type":"address"}],"name":"removeConnector","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IOracle","name":"oracle","type":"address"},{"internalType":"enum OffchainOracle.OracleType","name":"oracleKind","type":"uint8"}],"name":"removeOracle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract MultiWrapper","name":"_multiWrapper","type":"address"}],"name":"setMultiWrapper","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]'

contract_address = "0xfbD61B037C325b959c0F6A7e69D8f37770C2c550"

# Create the contract instance
contract_rate = w3.eth.contract(address=contract_address, abi=contract_abi_rate)


# Get the current time
current_time = int(time.time())
# Loop that runs once per minute
while True:
    current_time = int(time.time())
    ct = int(time.time()) - 60
    ct1 = 1
    print(ct)
    
    # Select all the tokens from the database whose time is more than one minute ago
    mycursor.execute("SELECT wallet_token, time FROM token WHERE abi IS not NULL and time < %s", (ct,)) 
    #tokens = mycursor.fetchall() 
   # sql1 = ("SELECT * FROM token WHERE `time` < %s")
    #val1 = (ct)
    #mycursor.execute(sql1, val1)
    tokens = mycursor.fetchall()
    sql = "SELECT wallet_token FROM token WHERE abi IS NULL"
    mycursor.execute(sql)
    tokens_abi = mycursor.fetchall()
    
    # Check if there are any wallets in the table with the value "1" in the "create" column
    mycursor.execute("SELECT * FROM tg_wallet WHERE ci = 1")
    wallets = mycursor.fetchall()
    
    # Select all the wallets from the database whose time is more than four minutes ago
    mycursor.execute("SELECT * FROM tg_wallet_bot WHERE ss = %s", (ct1,))
    wallets_tg = mycursor.fetchall()

    mycursor.execute("SELECT * FROM tran WHERE status = 0")
    transaction = mycursor.fetchone()
    
    for token_abi in tokens_abi:
        wallet_token_abi = token_abi[0]
        # Make a request to Etherscan API to get the ABI of the contract
        api_key = "Q8V4ANTUWXDIDWQPNJ5AH2KY7US7A2HRQG"
        url = f"https://api.bscscan.com/api?module=contract&action=getabi&address={wallet_token_abi}&apikey={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            # Update the 'abi' field in the database with the retrieved ABI
            abi = response.json()["result"]
            provider = HTTPProvider('https://bsc-dataseed.binance.org/')
            w3 = Web3(provider)
            w3.provider.chain_id = 56
            contract = w3.eth.contract(address=wallet_token_abi, abi=abi)
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            sql = "UPDATE token SET abi=%s, name_token=%s, decl=%s WHERE wallet_token=%s"
            val = (abi, symbol, decimals, wallet_token_abi)
            mycursor.execute(sql, val)
            mydb.commit()
        else:
            print(f"Failed to retrieve ABI for token {wallet_token_abi}")   
 
    for token in tokens:
        wallet_token = token[0]
        w3 = Web3()
        address = token[0]

        checksum_address = w3.toChecksumAddress(address)
        print(checksum_address)
        rate = contract_rate.functions.getRate(checksum_address, "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56", True).call()
        print("The rate of token with address", wallet_token, "is", rate)
        sql = "UPDATE token SET price=%s, time=%s WHERE wallet_token=%s"
        val = (rate, current_time, wallet_token)
        mycursor.execute(sql, val)
        mydb.commit()
         
        # If there are, create an Ethereum wallet for each    
    if wallets:
        for wallet in wallets:
            # Generate the private key
            private_key = w3.eth.account.create().privateKey.hex()
            # Generate the address from the private key
            address = w3.eth.account.privateKeyToAccount(private_key).address

            # Update the "wallet" and "key" columns in the database with the new address and private key
            sql = "UPDATE tg_wallet SET `wallet`=%s, `key`=%s, `ci`=0 WHERE id=%s"

            val = (address, private_key, wallet[0])
            mycursor.execute(sql, val)
            mydb.commit()

    for wallet in wallets_tg:
        wallet_address = wallet[2]
        json_data = {}
        mycursor.execute("SELECT wallet_token, name_token, abi, price FROM token")
        tokens = mycursor.fetchall()    
        api_key = "Q8V4ANTUWXDIDWQPNJ5AH2KY7US7A2HRQG"
        url = f"https://api.bscscan.com/api?module=account&action=balance&address={wallet_address}&apikey={api_key}"
        response_bnb = requests.get(url)

        if response_bnb.status_code == 200:
            # Update the 'abi' field in the database with the retrieved ABI
            balance_bnb = response_bnb.json()["result"]
            balance_bnb = int(balance_bnb) / 10**18
        else:
            print(f"Failed to retrieve ABI for token {wallet_address}")
              

        for token in tokens:
            # инициализация всего и вся
            web3 = Web3(Web3.HTTPProvider("https://bsc-dataseed1.defibit.io"))

            # ABI функции tokensBalance. Взять можно тут https://bscscan.com/address/0x83cb147c13cBA4Ba4a5228BfDE42c88c8F6881F6#code
            TOKENS_BALANCE_ABI = {"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address[]","name":"contracts","type":"address[]"}],"name":"tokensBalance","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"data","type":"bytes"}],"internalType":"struct BalanceScanner.Result[]","name":"results","type":"tuple[]"}],"stateMutability":"view","type":"function"}
            TOKENS_BALANCE_SELECTOR = encode_hex(function_abi_to_4byte_selector(TOKENS_BALANCE_ABI))
            tokens_balance_output_types = get_abi_output_types(TOKENS_BALANCE_ABI)

            # создать список адресов токенов для проверки
            tokens_to_check = []
            mycursor.execute("SELECT wallet_token FROM token")
            for token_address in mycursor.fetchall():
                tokens_to_check.append(token_address[0])

            # адрес пользователя, для которого будем смотреть баланс
            mycursor.execute("SELECT wallet FROM tg_wallet_bot WHERE id=%s", (wallet[0],))
            user_address = mycursor.fetchone()[0]
            encoded_data = encode_abi(
                web3=web3,
                abi=TOKENS_BALANCE_ABI,
                arguments=(user_address, [t for t in tokens_to_check]),  # аргументы функции tokensBalance
                data=TOKENS_BALANCE_SELECTOR,
            )
            tx = {
              "to": "0x83cb147c13cBA4Ba4a5228BfDE42c88c8F6881F6",  # адрес контракта BalanceScanner
              "data": encoded_data
            }
            # обращаемся к ноде
            tx_raw_data = web3.eth.call(tx)
            output_data = web3.codec.decode_abi(tokens_balance_output_types, tx_raw_data)[0]
            res = {}
            for token_address, (_, bytes_balance) in zip(tokens_to_check, output_data):
                wei_balance = web3.codec.decode_abi(["uint256"], HexBytes(bytes_balance))[0]
                res[token_address] = wei_balance
                
                
            # создать список адресов токенов для проверки
            tokens_to_check = []
            mycursor.execute("SELECT wallet_token, name_token, price, decl FROM token")
            for row in mycursor.fetchall():
                tokens_to_check.append(row)

            # адрес пользователя, для которого будем смотреть баланс
            mycursor.execute("SELECT id, wallet, json FROM tg_wallet_bot WHERE id=%s", (wallet[0],))
            user_id, user_address, json_wallet = mycursor.fetchone()

            # обновляем балансы в json кошельке
            wallet_dict = json.loads(json_wallet)
            for token_address, balance in res.items():
                token_info = next((t for t in tokens_to_check if t[0] == token_address), None)
                if token_info:
                    name, price, decimals = token_info[1:]
                    token_balance = balance / (10 ** decimals)
                    token_price = int(token_balance) * int(price) / 10 ** 18
                    wallet_dict[name] = {
                        "balance": "{:.6f}".format(token_balance),
                        "token_price": "{:.6f}".format(token_price)
                    }

            # обновляем json кошелек в базе данных
            json_wallet = json.dumps(wallet_dict)
            ss1 = 0
            mycursor.execute("UPDATE tg_wallet_bot SET `json`=%s, `time`=%s, `bnb`=%s, `ss`=%s WHERE id=%s", (json_wallet, current_time, balance_bnb, ss1, user_id))
            mydb.commit()
    if transaction:
        # Get transaction details from the database
        from_address = transaction[2]
        to_address = transaction[3]
        amount = transaction[4]
        token_address = transaction[5]

        # Get the private key from the tg_wallet table
        mycursor.execute("SELECT `key` FROM tg_wallet WHERE `wallet` = %s", (from_address,))
        private_key = mycursor.fetchone()[0]

        # Get the ABI and decl from the token table
        mycursor.execute("SELECT `abi`, `decl` FROM token WHERE `wallet_token` = %s", (token_address,))
        abi, decl = mycursor.fetchone()

        # Convert the token amount to the correct value
        token_amount = int(amount) * 10 ** int(decl)

        # Connect to the Binance Smart Chain
        provider = HTTPProvider('https://bsc-dataseed.binance.org/')
        w3 = Web3(provider)
        w3.provider.chain_id = 56
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # Create a contract instance
        contract_tra = w3.eth.contract(address=token_address, abi=abi)

        # Build the transaction dictionary
        nonce = w3.eth.getTransactionCount(from_address)
        bscTransfer_USDC = contract_tra.functions.transfer(w3.toChecksumAddress(to_address), w3.toWei(token_amount, 'wei')).buildTransaction({
            'chainId': 56,
            'from': from_address,
            'value': 0,
            'gas': 100000,
            'gasPrice': w3.toWei('5', 'gwei'),
            'nonce': nonce
        })

        # Подписываем транзакцию приватным ключом
        signed_txn = w3.eth.account.signTransaction(bscTransfer_USDC, private_key)

        # Отправляем подписанную транзакцию в сеть
        tx_hash = w3.eth.sendRawTransaction(signed_txn.rawTransaction)

        # Обновляем статус транзакции в базе данных
        mycursor.execute("UPDATE tran SET status = 1 WHERE id = %s", (transaction[0],))
        mydb.commit()

        print(f"Transaction sent: {tx_hash.hex()}")
    else:
        print("No pending transactions to execute.")
        
    # Iterate over each token and get its rate
    # Wait for one minute
    time.sleep(5)
