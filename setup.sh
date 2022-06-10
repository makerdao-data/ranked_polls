mkdir -p ~/.ranked-polls/

echo "\
[server]\n\
headless = true\n\
enableCORS=false\n\
port = $PORT\n\
" > ~/.ranked-polls/config.toml
