
VERSION=${1/v}
USER=${2}
echo "Version: ${VERSION}"
echo "Released version=${VERSION}" > RELEASE_NOTES.md
echo "Released by=${USER}" >> RELEASE_NOTES.md