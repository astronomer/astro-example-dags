import requests
import re
from packaging import version
import subprocess
import sys


def get_quay_tags(repo):
    """Returns a list of tags for a given Quay repository"""
    url = f"https://quay.io/api/v1/repository/{repo}/tag/?limit=100"
    response = requests.get(url)
    response.raise_for_status()
    tags = response.json()["tags"]
    return [tag["name"] for tag in tags if tag["name"]]


def filter_semver_tags(tags):
    """Filters out non-semver tags and sorts the list in descending order"""
    semver_tags = [tag for tag in tags if re.match(r"^\d+\.\d+\.\d+$", tag)]
    semver_tags.sort(key=version.parse, reverse=True)
    return semver_tags


def get_latest_version(tags):
    """Returns the latest version from a list of semver tags"""
    return tags[0] if tags else None


def get_version_from_dockerfile():
    """Returns the Astro Runtime version specified in the Dockerfile"""
    with open("Dockerfile", "r") as file:
        for line in file:
            if "astronomer/astro-runtime" in line:
                return line.split(":")[1].strip()


def update_dockerfile_and_prepare_commit(new_version):
    dockerfile_path = "../../Dockerfile"
    search_pattern = r"quay.io/astronomer/astro-runtime:[0-9]+\.[0-9]+\.[0-9]+"
    replacement = f"quay.io/astronomer/astro-runtime:{new_version}"

    with open(dockerfile_path, "r") as file:
        content = file.read()

    updated_content = re.sub(search_pattern, replacement, content)

    with open(dockerfile_path, "w") as file:
        file.write(updated_content)

    subprocess.run(["git", "config", "--global", "user.name", "GitHub Actions"])
    subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"])
    subprocess.run(["git", "add", dockerfile_path])
    subprocess.run(
        ["git", "commit", "-m", f"Update Dockerfile to version {new_version}"]
    )

    print("::set-output name=updated::true")
    print(f"::set-output name=new_version::{new_version}")


def main():
    quay_repo = "astronomer/astro-runtime"
    tags = get_quay_tags(quay_repo)
    semver_tags = filter_semver_tags(tags)
    latest_version = get_latest_version(semver_tags)
    dockerfile_version = get_version_from_dockerfile()

    print(f"Latest Runtime version: {latest_version}")
    print(f"Current Dockerfile version: {dockerfile_version}")

    if latest_version == dockerfile_version:
        print("Dockerfile is up to date! :)")
    else:
        print("Dockerfile is outdated!")
        print(f"PRing an update to the Dockerfile to version {latest_version}")
        update_dockerfile_and_prepare_commit(latest_version)


if __name__ == "__main__":
    main()
