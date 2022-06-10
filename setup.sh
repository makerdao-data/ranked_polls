mkdir -p ~/.ranked_algo/

echo "\
[server]\n\
headless = true\n\
enableCORS=false\n\
port = $PORT\n\
" > ~/.ranked_algo/config.toml
