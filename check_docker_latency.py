import time
import requests
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION ---
MODEL = "qwen2.5:7b"
URL = "http://localhost:11434/api/generate"
# We use a real extraction prompt to simulate your actual project work
PROMPT = "Extract: James Joseph, 15, Head of Elk. Format: Name|Age|City|State"
NUM_REQUESTS = 4  # Total rows to simulate
MAX_WORKERS = 2   # Number of rows processed at the same time

def measure_single_request(request_id):
    """Function to send a single request and return timing metrics."""
    payload = {
        "model": MODEL,
        "prompt": PROMPT,
        "stream": False,
        "options": {
            "num_predict": 50,  # Keep output short to save time
            "temperature": 0
        }
    }
    
    start_time = time.perf_counter()
    try:
        # We use a 60s timeout because CPU inference is slow
        response = requests.post(URL, json=payload, timeout=60)
        end_time = time.perf_counter()
        
        if response.status_code == 200:
            data = response.json()
            total_time = end_time - start_time
            # Ollama internal metrics (nanoseconds to seconds)
            eval_duration = data.get('eval_duration', 1) / 1e9
            token_count = data.get('eval_count', 0)
            tps = token_count / eval_duration if eval_duration > 0 else 0
            
            return {
                "id": request_id,
                "total": total_time,
                "gen_time": eval_duration,
                "tps": tps,
                "success": True
            }
    except Exception as e:
        return {"id": request_id, "success": False, "error": str(e)}

def run_parallel_test():
    print(f"--- Starting Parallel Latency Test ---")
    print(f"Simulating {NUM_REQUESTS} rows using {MAX_WORKERS} parallel workers...")
    
    overall_start = time.perf_counter()
    
    # This is the engine that runs requests in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(measure_single_request, range(NUM_REQUESTS)))
    
    overall_end = time.perf_counter()
    total_clock_time = overall_end - overall_start
    
    # Filter successful runs
    successes = [r for r in results if r['success']]
    
    if successes:
        avg_lat = sum(r['total'] for r in successes) / len(successes)
        avg_tps = sum(r['tps'] for r in successes) / len(successes)
        
        print("\nDetailed Breakdown:")
        for r in results:
            if r['success']:
                print(f" Row {r['id']}: Time={r['total']:.2f}s | Speed={r['tps']:.2f} t/s")
            else:
                print(f" Row {r['id']}: FAILED ({r['error']})")
        
        print("-" * 50)
        print(f"Average Latency:  {avg_lat:.2f}s per row")
        print(f"Average Speed:    {avg_tps:.2f} tokens/sec")
        print(f"TOTAL CLOCK TIME: {total_clock_time:.2f}s to finish {NUM_REQUESTS} rows")
        
        # This tells you how much time you're actually saving
        theoretical_sequential = avg_lat * NUM_REQUESTS
        savings = theoretical_sequential - total_clock_time
        print(f"Time Saved vs Sequential: {savings:.2f}s")
    else:
        print("Error: No successful requests. Is Docker/Ollama running?")

if __name__ == "__main__":
    run_parallel_test()