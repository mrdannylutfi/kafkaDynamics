# Contributing to This Project

First off, thank you for taking the time to contribute! It is people like you who make open-source projects better.

Please review the following guidelines to make your contributing process smooth and effective.

## Table of Contents
- [How to Report a Bug](#how-to-report-a-bug)
- [How to Suggest a Feature](#how-to-suggest-a-feature)
- [Local Development Setup](#local-development-setup)
- [Style Guide & Best Practices](#style-guide--best-practices)
- [Pull Request Process](#pull-request-process)

---

## How to Report a Bug
If you find a bug, please open an **Issue** on GitHub and include:
* A clear, descriptive title.
* Steps to reproduce the issue.
* Expected behavior vs. actual behavior.
* Your compiler version and operating system details.

## How to Suggest a Feature
We welcome ideas for new features! To suggest one:
* Open an **Issue** and label it as a "feature request".
* Explain the problem this feature solves and why it is useful.
* Describe how you imagine the feature working.

## Local Development Setup
To set up the project locally for code modifications:

1. **Fork the repository** on GitHub.
2. **Clone your fork** to your local machine:
   ```bash
   git clone https://github.com
   ```
3. **Create a new branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Style Guide & Best Practices
Since this is a C project, please keep the following structural habits in mind to ensure code readability:
* **Memory Management:** Ensure all dynamically allocated memory (`malloc`, `calloc`) is properly freed (`free`) to avoid memory leaks.
* **Readability:** Use clear variable names and comment complex logic, pointers, or custom data structures.
* **Consistency:** Try to follow the indentation and bracket placement styles already present in the existing `.c` and `.h` files.

## Pull Request Process
When you are ready to submit your changes:

1. **Push your branch** to your GitHub fork.
2. **Open a Pull Request (PR)** against our `main` or `development` branch.
3. **Describe your changes** clearly in the PR description so reviewers understand what you did.
4. Keep an eye out for any reviewer feedback or questions.
