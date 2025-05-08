import subprocess
import time
import sys



def main():
    """
    Continuously runs the command:
        python3 -m 01_retrieve_data.retrieve_data
    """
    cmd = [sys.executable, "-m", "01_retrieve_data.retrieve_data"]

    while True:
        try:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Running retrieve_data...")
            subprocess.run(cmd, check=True)
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Completed successfully. Restarting immediately.")


        except subprocess.CalledProcessError as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error (exit code {e.returncode}). Retrying in 10 seconds...")
            time.sleep(10)
        
        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting.")
            break


if __name__ == "__main__":
    main()