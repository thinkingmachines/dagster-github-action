from setuptools import find_packages, setup


if __name__ == "__main__":
    setup(
        name="dagster-github-action",
        version="0.1.0",
        author="Thinking Machines",
        author_email="engineering@thinkingmachin.es",
        license="Apache-2.0",
        description="A Dagster integration for GitHub Actions",
        url="https://github.com/thinkingmachines/dagster-github-action",
        classifiers=[
            "Programming Language :: Python :: 3.8",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
        ],
        packages=find_packages(exclude=["test"]),
        install_requires=[
            "dagster",
            "dagster_graphql",
        ],
        tests_require=[],
        zip_safe=False,
    )