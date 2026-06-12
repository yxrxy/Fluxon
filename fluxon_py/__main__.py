#!/usr/bin/env python3
"""
KV Cache API Layer - Main Entry Point
Start KV Cache API layer service directly.
"""

import argparse
from math import log
import sys
import os
import yaml
from pathlib import Path
from . import (
    FluxonKvClientConfig,  
    __version__
)
from .kvclient import new_store
from .config import FluxonKvClientConfig, _yaml_template
import logging

def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description='KV Cache API Layer - distributed key-value cache service',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage:
  # Start the KV cache server
  python -m fluxon_py --server
  
  # Start with a custom config
  python -m fluxon_py --server --config my_config.yaml
        """
    )
    
    parser.add_argument('--config', '-c', default='config.yaml', 
                       help='Config file path (default: config.yaml)')
    parser.add_argument('--server', '-s', action='store_true',
                      help='Start KV cache server')
    
    args = parser.parse_args()
    
    if not args.server:
        parser.print_help()
        return
    
    success = run_server(args.config)
    sys.exit(0 if success else 1)

def run_server(config_path: str):
    """Run KV cache server."""
    logging.info(f"=== KV Cache API Layer Server v{__version__} ===")
    
    # Verify config file exists
    if not check_config_exists(config_path):
        return False
    
    
    # Load config
    logging.info(f"📋 Loading config file: {config_path}")
    try:
        raw_text = Path(config_path).read_text(encoding="utf-8")
        try:
            config_dict = yaml.safe_load(raw_text)
        except yaml.YAMLError as e:
            # English note: print the full document to make YAML syntax errors actionable.
            print(
                f"YAML parse failed: source={config_path}\n--- YAML BEGIN ---\n{raw_text}\n--- YAML END ---",
                file=sys.stderr,
            )
            logging.warning(f"❌ Failed to parse YAML config: {e}")
            return False
        config = FluxonKvClientConfig(config_dict)
        logging.info("✅ Config loaded")
        logging.info(config)
        
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        logging.warning(f"❌ Failed to load config: {e}")
        return False
    
    # Create store based on config
    logging.info("🚀 Starting KV cache service...")
    try:
        # Create store
        store_result = new_store(config)
        if not store_result.is_ok():
            logging.warning(f"❌ Service initialization failed: {store_result.unwrap_error()}")
            return False
            
        store = store_result.unwrap()
        logging.info("✅ KV cache service started")
        
        # Event-driven server loop
        import signal
        import threading
        
        # Stop event
        stop_event = threading.Event()
        
        def signal_handler(signum, frame):
            logging.info("\n🛑 Received stop signal; shutting down...")
            stop_event.set()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logging.info("🎉 KV cache service is running...")
        logging.info("Press Ctrl+C to stop")
        
        try:
            # Wait for stop event instead of busy polling
            stop_event.wait()
        finally:
            close_result = store.close()
            if not close_result.is_ok():
                logging.warning(f"⚠️ Error during shutdown: {close_result.unwrap_error()}")
            else:
                _ = close_result.unwrap()
                logging.info("✅ Service stopped")
        
        return True
            
    except Exception as e:
        logging.warning(f"❌ Service failed to start: {e}")
        return False

def check_config_exists(config_path: str) -> bool:
    """Check whether config file exists; if missing, print a template and return False."""
    if not os.path.exists(config_path):
        logging.warning(f"❌ Config file does not exist: {config_path}")
        logging.warning(f"\nPlease create {config_path} with content like:")
        logging.warning("=" * 60)
                # 3. Use the __str__ representation to get the formatted string.
        # The __str__ method produces a commented-out, human-readable format.
        # We will use that as the template.
        logging.warning("Config template:\n%s", _yaml_template())

        logging.warning("=" * 60)

        return False
    return True





if __name__ == "__main__":
    main() 
