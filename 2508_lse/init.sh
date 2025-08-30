source ./env.sh
machadmin -k; echo 'y'|machadmin -d; machadmin -c; machadmin -u
machsql -s localhost -u sys -p manager -f create_table.sql
make
./append
machsql -s localhost -u sys -p manager -f query.sql
