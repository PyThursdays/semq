from setuptools import setup, find_packages


with open("requirements.txt", "r") as file:
    requirements = [
        req
        for req in file.read().splitlines()
        if req and not req.startswith("#")
    ]


setup(
    name="semq",
    version="0.1.0",
    description="Simple External Memory Queue",
    packages=find_packages(where="src"),
    install_requires=requirements,
    package_dir={
        "": "src"
    },
)

