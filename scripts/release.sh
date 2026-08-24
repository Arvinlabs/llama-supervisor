#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting release process...${NC}"

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}Error: Not in a git repository${NC}"
    exit 1
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes${NC}"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Get current version
CURRENT_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
echo -e "Current version: ${GREEN}${CURRENT_TAG}${NC}"

# Parse version
IFS='.' read -ra VERSION_PARTS <<< "${CURRENT_TAG#v}"
MAJOR=${VERSION_PARTS[0]}
MINOR=${VERSION_PARTS[1]}
PATCH=${VERSION_PARTS[2]}

# Ask for version bump type
echo -e "\n${YELLOW}Select version bump type:${NC}"
echo "1) Major (v$((MAJOR + 1)).0.0)"
echo "2) Minor (v${MAJOR}.$((MINOR + 1)).0)"
echo "3) Patch (v${MAJOR}.${MINOR}.$((PATCH + 1)))"
echo "4) Custom"

read -p "Choice (1-4): " BUMP_TYPE

case $BUMP_TYPE in
    1)
        NEW_MAJOR=$((MAJOR + 1))
        NEW_MINOR=0
        NEW_PATCH=0
        ;;
    2)
        NEW_MAJOR=$MAJOR
        NEW_MINOR=$((MINOR + 1))
        NEW_PATCH=0
        ;;
    3)
        NEW_MAJOR=$MAJOR
        NEW_MINOR=$MINOR
        NEW_PATCH=$((PATCH + 1))
        ;;
    4)
        read -p "Enter custom version (format: vX.Y.Z): " CUSTOM_VERSION
        if [[ ! $CUSTOM_VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo -e "${RED}Error: Invalid version format. Use vX.Y.Z${NC}"
            exit 1
        fi
        NEW_VERSION=$CUSTOM_VERSION
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

if [ -z "$NEW_VERSION" ]; then
    NEW_VERSION="v${NEW_MAJOR}.${NEW_MINOR}.${NEW_PATCH}"
fi

echo -e "\n${YELLOW}New version will be: ${GREEN}${NEW_VERSION}${NC}"

# Confirm
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Release cancelled${NC}"
    exit 0
fi

# Create git tag
echo -e "\n${GREEN}Creating git tag ${NEW_VERSION}...${NC}"
git tag -a "${NEW_VERSION}" -m "Release ${NEW_VERSION}"

# Push tag
echo -e "\n${GREEN}Pushing tag to remote...${NC}"
git push origin main --tags

echo -e "\n${GREEN}✅ Release ${NEW_VERSION} created successfully!${NC}"
echo -e "\nNext steps:"
echo "1. GitHub Actions will automatically create a release with binaries"
echo "2. Check the releases page: https://github.com/Arvinlabs/llama-supervisor/releases"
echo "3. Verify all binaries work correctly"
