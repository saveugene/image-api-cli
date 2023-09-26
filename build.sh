base_name=image-api-cli

for GOOS in darwin windows linux
do
    for GOARCH in amd64 arm64 386
    do
        if [ "$GOOS" == "darwin" ] && [ "$GOARCH" == "386" ]; then
            continue
        fi
        GOOS=$GOOS GOARCH=$GOARCH go build -o bin/$base_name-$GOOS\_$GOARCH main.go
    done
done
