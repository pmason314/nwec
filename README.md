# NWEC Data Analysis

Volunteer data analysis, pipeline automation, and scripting done for the Northwest Energy Coalition.


## Setup

1. Set up git and SSH keys from the [GitHub instructions](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key).
2. Clone this repository:
    ```
    git clone git@github.com:pmason314/nwec.git
    ```
3. `cd` into the repository folder and install `uv` for Python and dependency management:
    ```
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
4. Create the project's virtual environment:
   ```
    uv venv --python-preference only-managed
   ```
5. Install the project's dependencies:
    ```
    uv sync
    ```

## Usage

(In progress)
