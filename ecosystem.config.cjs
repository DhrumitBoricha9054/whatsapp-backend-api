module.exports = {
  apps: [{
    name: 'whatsapp-api',
    script: './server.js', // Change this to your main app file (could be index.js, server.js, etc.)
    instances: 1, // Single instance for file uploads
    exec_mode: 'fork',
    max_memory_restart: '1G',
    node_args: '--max-old-space-size=2048',
    env: {
      NODE_ENV: 'production',
      UV_THREADPOOL_SIZE: 16 // Increase thread pool for file operations
    },
    // Additional production settings
    error_file: './logs/err.log',
    out_file: './logs/out.log',
    log_file: './logs/combined.log',
    time: true,
    autorestart: true,
    watch: false,
    max_restarts: 10,
    min_uptime: '10s'
  }]
};