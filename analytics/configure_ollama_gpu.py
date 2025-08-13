#!/usr/bin/env python3
"""
Ollama GPU Configuration Script
Configures Ollama to properly use NVIDIA GPU acceleration
"""

import os
import subprocess
import time
import signal
import sys

def check_gpu_available():
    """Check if NVIDIA GPU is available"""
    try:
        result = subprocess.run(['nvidia-smi'], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def check_cuda_available():
    """Check if CUDA libraries are available"""
    try:
        result = subprocess.run(['ldconfig', '-p'], capture_output=True, text=True)
        return 'libcuda.so' in result.stdout
    except:
        return False

def kill_existing_ollama():
    """Kill existing Ollama processes"""
    try:
        # Find Ollama processes
        result = subprocess.run(['pgrep', '-f', 'ollama'], capture_output=True, text=True)
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            print(f"Found {len(pids)} Ollama processes to terminate")
            
            for pid in pids:
                if pid.strip():
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        print(f"Terminated process {pid}")
                    except ProcessLookupError:
                        print(f"Process {pid} already terminated")
                    except PermissionError:
                        print(f"Permission denied to kill process {pid}")
            
            # Wait for processes to terminate
            time.sleep(3)
            
            # Force kill if still running
            result = subprocess.run(['pgrep', '-f', 'ollama'], capture_output=True, text=True)
            if result.returncode == 0:
                remaining_pids = result.stdout.strip().split('\n')
                for pid in remaining_pids:
                    if pid.strip():
                        try:
                            os.kill(int(pid), signal.SIGKILL)
                            print(f"Force killed process {pid}")
                        except:
                            pass
    except Exception as e:
        print(f"Error killing Ollama processes: {e}")

def start_ollama_with_gpu():
    """Start Ollama with GPU configuration"""
    print("\n🚀 Starting Ollama with GPU Configuration")
    print("=" * 50)
    
    # Set GPU environment variables
    gpu_env = os.environ.copy()
    gpu_env.update({
        'CUDA_VISIBLE_DEVICES': '0',
        'OLLAMA_GPU_LAYERS': '99',
        'OLLAMA_GPU_MEMORY': '8192',
        'OLLAMA_NUM_PARALLEL': '4',
        'OLLAMA_MAX_LOADED_MODELS': '1',
        'OLLAMA_FLASH_ATTENTION': '1',
        'OLLAMA_KV_CACHE_TYPE': 'f16',
        'OLLAMA_HOST': '0.0.0.0:11434',
        'OLLAMA_DEBUG': '1'
    })
    
    print("🔧 GPU Environment Variables:")
    for key, value in gpu_env.items():
        if key.startswith('OLLAMA_') or key.startswith('CUDA_'):
            print(f"   {key}={value}")
    
    try:
        # Start Ollama serve in background
        print("\n🔄 Starting Ollama server...")
        process = subprocess.Popen(
            ['ollama', 'serve'],
            env=gpu_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for server to start
        time.sleep(5)
        
        # Check if process is still running
        if process.poll() is None:
            print("✅ Ollama server started successfully")
            return process
        else:
            stdout, stderr = process.communicate()
            print(f"❌ Ollama server failed to start")
            print(f"STDOUT: {stdout}")
            print(f"STDERR: {stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Error starting Ollama: {e}")
        return None

def test_gpu_inference():
    """Test GPU inference with a simple query"""
    print("\n🧪 Testing GPU Inference")
    print("=" * 30)
    
    try:
        # Set environment for client
        env = os.environ.copy()
        env.update({
            'CUDA_VISIBLE_DEVICES': '0',
            'OLLAMA_GPU_LAYERS': '99'
        })
        
        # Run a simple test
        start_time = time.time()
        result = subprocess.run(
            ['ollama', 'run', 'llama3.2:3b', 'Hello, test GPU'],
            env=env,
            capture_output=True,
            text=True,
            timeout=60
        )
        end_time = time.time()
        
        if result.returncode == 0:
            duration = end_time - start_time
            print(f"✅ GPU test completed in {duration:.2f} seconds")
            print(f"📝 Response: {result.stdout[:200]}...")
            
            # Check if it's using GPU (faster response indicates GPU usage)
            if duration < 30:
                print("🚀 Fast response - likely using GPU!")
            else:
                print("⚠️  Slow response - may be using CPU")
                
            return True
        else:
            print(f"❌ Test failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Test timed out")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def check_ollama_status():
    """Check current Ollama status"""
    print("\n📊 Checking Ollama Status")
    print("=" * 30)
    
    try:
        # Check running models
        result = subprocess.run(['ollama', 'ps'], capture_output=True, text=True)
        if result.returncode == 0:
            print("📋 Running models:")
            print(result.stdout)
            
            # Check if using GPU or CPU
            if 'CPU' in result.stdout:
                print("⚠️  Model is using CPU")
                return False
            elif 'GPU' in result.stdout:
                print("✅ Model is using GPU")
                return True
            else:
                print("❓ Processor type unclear")
                return None
        else:
            print("❌ Failed to get Ollama status")
            return False
            
    except Exception as e:
        print(f"❌ Error checking status: {e}")
        return False

def main():
    print("🔧 Ollama GPU Configuration Tool")
    print("=" * 40)
    
    # Check prerequisites
    if not check_gpu_available():
        print("❌ NVIDIA GPU not available")
        return False
        
    if not check_cuda_available():
        print("❌ CUDA libraries not available")
        return False
        
    print("✅ GPU and CUDA available")
    
    # Kill existing Ollama processes
    kill_existing_ollama()
    
    # Start Ollama with GPU
    process = start_ollama_with_gpu()
    if not process:
        return False
    
    try:
        # Test GPU inference
        if test_gpu_inference():
            print("\n🎉 Ollama GPU configuration successful!")
        else:
            print("\n⚠️  GPU test failed, but server is running")
        
        # Check final status
        check_ollama_status()
        
        print("\n📝 Ollama server is running with GPU configuration")
        print("   Use Ctrl+C to stop the server")
        
        # Keep server running
        try:
            process.wait()
        except KeyboardInterrupt:
            print("\n🛑 Stopping Ollama server...")
            process.terminate()
            process.wait()
            
    except Exception as e:
        print(f"❌ Error during operation: {e}")
        if process:
            process.terminate()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)