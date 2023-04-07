for i in {10..400}; do
  curl -s -k 'POST' -H 'header info'  'http://localhost:9780/set?key=keyNNew'$i -d ''$i
done