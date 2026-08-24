#!/bin/bash

set -e

BINARY="llama-supervisor"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Version Information${NC}"
echo "=================="

# Get git version
GIT_VERSION=$(git rev-parse --short HEAD)
GIT_TAG=$(git describe --tags --always --dirty 2>/dev/null | sed 's/-g\([0-9a-f]\)/-\1/' || echo "dev-${GIT_VERSION}")

echo -e "Git Version: ${YELLOW}${GIT_TAG}${NC}"
echo -e "Commit Hash: ${YELLOW}${GIT_VERSION}${NC}"

# Get go version
GO_VERSION=$(go version | awk '{print $3}')
echo -e "Go Version:  ${YELLOW}${GO_VERSION}${NC}"

# Get build info from binary if it exists
if [ -f "dist/${BINARY}" ]; then
    echo -e "\n${GREEN}Binary Information${NC}"
    echo "=================="

    # Try to get version from binary
    BINARY_VERSION=$(dist/${BINARY} --version 2>/dev/null || echo "N/A")
    echo -e "Binary Version: ${YELLOW}${BINARY_VERSION}${NC}"

    # File info
    FILE_SIZE=$(ls -lh dist/${BINARY} | awk '{print $5}')
    echo -e "File Size:     ${YELLOW}${FILE_SIZE}${NC}"

    # Architecture
    if command -v file &> /dev/null; then
        ARCH=$(file dist/${BINARY} | grep -o "ELF.*\|Mach-O.*")
        echo -e "Architecture:  ${YELLOW}${ARCH}${NC}"
    fi
fi

# List all available binaries
if [ -d "dist" ]; then
    echo -e "\n${GREEN}Available Binaries${NC}"
    echo "=================="
    ls -la dist/ 2>/dev/null | grep -v "^total" || echo "No binaries found"
fi

# Show recent tags
echo -e "\n${GREEN}Recent Tags${NC}"
echo "=========="
git tag --sort=-v:refname | head -5
