from flask import Flask, jsonify, Response
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import time
import concurrent.futures
import json
import io

app = Flask(__name__)

# Hàm helper để chuyển đổi numpy types sang Python types
def convert_numpy_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict('records')
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    return obj

# Class JSONEncoder tùy chỉnh để xử lý các kiểu dữ liệu numpy
class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyJSONEncoder, self).default(obj)

app.json_encoder = NumpyJSONEncoder

# Danh sách blockchain
CHAINS = [
    "Ethereum", "Solana", "Near", "Bitcoin", "Sui", 
    "Aptos", "Arbitrum", "Sei", "Base", "BSC", 
    "Polygon", "Optimism", "Fantom", "Avalanche", "Celo"
]

# API URL template
API_URL_TEMPLATE = "https://tvl-defillama-service-1094890588015.us-central1.run.app/tvl/{chain_id}"

# Cache dữ liệu để không phải gọi API mỗi lần
tvl_cache = {}
cache_time = {}

def fetch_tvl_data(chain_id):
    """Fetches TVL data from the DeFi Llama API for a specific chain."""
    try:
        # Kiểm tra cache
        current_time = time.time()
        if chain_id in cache_time and current_time - cache_time[chain_id] < 3600:  # Cache 1 giờ
            return tvl_cache.get(chain_id)
            
        api_url = API_URL_TEMPLATE.format(chain_id=chain_id)
        response = requests.get(api_url)
        response.raise_for_status()
        
        # Lưu vào cache
        tvl_cache[chain_id] = response.json()
        cache_time[chain_id] = current_time
        
        return response.json()
    except Exception as e:
        print(f"Error fetching TVL data for {chain_id}: {e}")
        return None

def process_tvl_data(data, chain_id):
    """Processes the TVL API data and returns a pandas DataFrame."""
    try:
        if not data:
            return None
            
        # Convert JSON to DataFrame
        df = pd.DataFrame(data)
        
        # Convert Unix timestamp to datetime
        df['date_time'] = pd.to_datetime(df['date'], unit='s')
        df['date'] = df['date_time'].dt.strftime('%Y-%m-%d')
        
        # Add chain column
        df['chain'] = chain_id
        
        # Convert numeric columns to float
        df['tvl'] = pd.to_numeric(df['tvl'], errors='coerce')
        
        return df
    except Exception as e:
        print(f"Error processing TVL data for {chain_id}: {e}")
        return None

@app.route('/api/tvl/<chain_id>', methods=['GET'])
def get_tvl_for_chain(chain_id):
    """API endpoint to get TVL data for a specific chain."""
    if chain_id not in CHAINS:
        return jsonify({"error": f"Invalid chain ID. Supported chains: {', '.join(CHAINS)}"}), 400
        
    data = fetch_tvl_data(chain_id)
    if not data:
        return jsonify({"error": f"Failed to fetch data for {chain_id}"}), 500
        
    df = process_tvl_data(data, chain_id)
    if df is None or df.empty:
        return jsonify({"error": f"Failed to process data for {chain_id}"}), 500
    
    # Lấy dữ liệu mới nhất
    latest = df.sort_values('date', ascending=False).iloc[0]
    
    # Tính phần trăm thay đổi 24h
    if len(df) > 1:
        yesterday = df.sort_values('date', ascending=False).iloc[1]
        change = latest['tvl'] - yesterday['tvl']
        percent_change = (change / yesterday['tvl']) * 100 if yesterday['tvl'] > 0 else 0
    else:
        change = 0
        percent_change = 0
    
    # Chuyển đổi giá trị numpy sang Python types
    history_data = df[['date', 'tvl']].sort_values('date', ascending=False).head(30).to_dict('records')
    history_data = [
        {"date": item["date"], "tvl": float(item["tvl"])} 
        for item in history_data
    ]
    
    result = {
        "chain": chain_id,
        "latest_date": str(latest['date']),
        "tvl": float(latest['tvl']),
        "tvl_change_24h": float(change),
        "tvl_percent_change_24h": float(percent_change),
        "history": history_data
    }
    
    return jsonify(result)

@app.route('/api/tvl/all', methods=['GET'])
def get_all_tvl():
    """API endpoint to get latest TVL data for all chains."""
    results = []
    total_tvl = 0
    
    # Fetch data for all chains in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_chain = {executor.submit(fetch_tvl_data, chain): chain for chain in CHAINS}
        
        for future in concurrent.futures.as_completed(future_to_chain):
            chain = future_to_chain[future]
            try:
                data = future.result()
                if data:
                    df = process_tvl_data(data, chain)
                    if df is not None and not df.empty:
                        latest = df.sort_values('date', ascending=False).iloc[0]
                        
                        # Tính phần trăm thay đổi 24h
                        if len(df) > 1:
                            yesterday = df.sort_values('date', ascending=False).iloc[1]
                            change = latest['tvl'] - yesterday['tvl']
                            percent_change = (change / yesterday['tvl']) * 100 if yesterday['tvl'] > 0 else 0
                        else:
                            change = 0
                            percent_change = 0
                        
                        # Chuyển đổi history data
                        history_data = df[['date', 'tvl']].sort_values('date', ascending=False).head(30).to_dict('records')
                        history_data = [
                            {"date": item["date"], "tvl": float(item["tvl"])} 
                            for item in history_data
                        ]
                        
                        # Chuyển đổi numpy values sang Python types
                        chain_result = {
                            "chain": chain,
                            "latest_date": str(latest['date']),
                            "tvl": float(latest['tvl']),
                            "tvl_change_24h": float(change),
                            "tvl_percent_change_24h": float(percent_change),
                            "history": history_data
                        }
                        
                        results.append(chain_result)
                        total_tvl += float(latest['tvl'])
            except Exception as e:
                print(f"Error processing {chain}: {e}")
    
    # Sắp xếp kết quả theo TVL giảm dần
    results.sort(key=lambda x: x['tvl'], reverse=True)
    
    return jsonify({
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_tvl": float(total_tvl),
        "chains": results
    })

@app.route('/api/tvl/csv', methods=['GET'])
def get_tvl_csv():
    """API endpoint to get TVL data for all chains in CSV format."""
    all_data = []
    
    # Fetch data for all chains in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_chain = {executor.submit(fetch_tvl_data, chain): chain for chain in CHAINS}
        
        for future in concurrent.futures.as_completed(future_to_chain):
            chain = future_to_chain[future]
            try:
                data = future.result()
                if data:
                    df = process_tvl_data(data, chain)
                    if df is not None and not df.empty:
                        # Chỉ lấy các cột cần thiết: chain, date và tvl
                        chain_df = df[['chain', 'date', 'tvl']].copy()
                        all_data.append(chain_df)
            except Exception as e:
                print(f"Error processing {chain} for CSV: {e}")
    
    if not all_data:
        return jsonify({"error": "No data available to export"}), 500
    
    # Kết hợp tất cả dữ liệu
    combined_df = pd.concat(all_data)
    
    # Sắp xếp dữ liệu theo chain và date
    combined_df = combined_df.sort_values(['chain', 'date'])
    
    # Tạo CSV trong bộ nhớ
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    
    # Tạo response với CSV
    response = Response(
        csv_buffer.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=tvl_data.csv"}
    )
    
    return response

@app.route('/', methods=['GET'])
def home():
    return """
    <html>
        <head>
            <title>TVL API</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #2c3e50; }
                .endpoint { background: #f8f9fa; padding: 10px; margin: 10px 0; border-radius: 5px; }
                code { background: #e9ecef; padding: 2px 5px; border-radius: 3px; }
            </style>
        </head>
        <body>
            <h1>TVL API</h1>
            <p>Sử dụng API này để lấy dữ liệu TVL từ DeFi Llama.</p>
            
            <div class="endpoint">
                <h3>Lấy dữ liệu cho một blockchain:</h3>
                <code>GET /api/tvl/{chain_id}</code>
                <p>Ví dụ: <a href="/api/tvl/Ethereum">/api/tvl/Ethereum</a></p>
            </div>
            
            <div class="endpoint">
                <h3>Lấy dữ liệu cho tất cả blockchain:</h3>
                <code>GET /api/tvl/all</code>
                <p>Ví dụ: <a href="/api/tvl/all">/api/tvl/all</a></p>
            </div>
            
            <div class="endpoint">
                <h3>Tải xuống dữ liệu dạng CSV:</h3>
                <code>GET /api/tvl/csv</code>
                <p>Tải xuống dữ liệu TVL của tất cả blockchain dưới dạng CSV: <a href="/api/tvl/csv">/api/tvl/csv</a></p>
            </div>
            
            <h3>Blockchain hỗ trợ:</h3>
            <ul>
                """ + "".join([f"<li><a href='/api/tvl/{chain}'>{chain}</a></li>" for chain in CHAINS]) + """
            </ul>
        </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)