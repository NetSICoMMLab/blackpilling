cat * | jq -r '.[2]' | sort | uniq -c > unique_authors.csv
