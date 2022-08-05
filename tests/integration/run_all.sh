test_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

(
    cd $test_dir;
    go get -u -t ./...;
    go mod download;
    go mod tidy;
)

go test -race -count=1 -v $test_dir
