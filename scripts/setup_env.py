#!/usr/bin/env python
"""Generic development environment setup script for AMI projects."""
import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional  # noqa: UP035


class EnvironmentSetup:
    """Handle development environment setup for AMI projects."""

    def __init__(self, project_root: Optional[Path] = None, project_name: Optional[str] = None):  # noqa: UP007
        """Initialize the setup.

        Args:
            project_root: Root directory of the project. Defaults to current directory.
            project_name: Name of the project for display. Defaults to directory name.
        """
        self.project_root = project_root or Path.cwd()
        self.project_name = project_name or self.project_root.name
        self.venv_path = self.project_root / ".venv"

        # Determine Python executable for venv
        if sys.platform == "win32":
            self.venv_python = self.venv_path / "Scripts" / "python.exe"
            self.venv_activate = self.venv_path / "Scripts" / "activate"
        else:
            self.venv_python = self.venv_path / "bin" / "python"
            self.venv_activate = f"source {self.venv_path / 'bin' / 'activate'}"

    def check_uv(self) -> bool:
        """Check if uv is installed."""
        try:
            subprocess.run(["uv", "--version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("[ERROR] uv is not installed!")
            print("Install it with: pip install uv")
            return False

    def create_venv(self) -> bool:
        """Create virtual environment if it doesn't exist."""
        if not self.venv_path.exists():
            print(f"\n[1/5] Creating virtual environment at {self.venv_path}...")
            try:
                subprocess.run(["uv", "venv", str(self.venv_path), "--python", "python3.12"], check=True, cwd=self.project_root)
                print("[OK] Virtual environment created")
                return True
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Failed to create virtual environment: {e}")
                return False
        else:
            print("\n[1/5] Virtual environment already exists")
            return True

    def install_requirements(self, requirements_file: str = "requirements.txt") -> bool:
        """Install dependencies from requirements file."""
        # First install base requirements if this is a submodule
        parent_base = self.project_root.parent / "base"
        if parent_base.exists():
            base_req = parent_base / requirements_file
            if base_req.exists():
                print(f"\n[2a/5] Installing base dependencies from {base_req.relative_to(self.project_root.parent)}...")
                try:
                    subprocess.run(["uv", "pip", "install", "--python", str(self.venv_python), "-r", str(base_req)], check=True, cwd=self.project_root)
                    print("[OK] Base dependencies installed")
                except subprocess.CalledProcessError as e:
                    print(f"[ERROR] Failed to install base dependencies: {e}")
                    return False

        # Then install project requirements
        req_path = self.project_root / requirements_file
        if req_path.exists():
            print(f"\n[2b/5] Installing project dependencies from {requirements_file}...")
            try:
                subprocess.run(["uv", "pip", "install", "--python", str(self.venv_python), "-r", str(req_path)], check=True, cwd=self.project_root)
                print("[OK] Project dependencies installed")
                return True
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Failed to install project dependencies: {e}")
                return False
        else:
            print(f"\n[2b/5] No {requirements_file} found - skipping")
            return True

    def install_test_requirements(self, test_requirements_file: str = "requirements-test.txt") -> bool:
        """Install test dependencies."""
        # First install base test requirements if this is a submodule
        parent_base = self.project_root.parent / "base"
        if parent_base.exists():
            base_test_req = parent_base / test_requirements_file
            if base_test_req.exists():
                print(f"\n[3a/5] Installing base test dependencies from {base_test_req.relative_to(self.project_root.parent)}...")
                try:
                    subprocess.run(["uv", "pip", "install", "--python", str(self.venv_python), "-r", str(base_test_req)], check=True, cwd=self.project_root)
                    print("[OK] Base test dependencies installed")
                except subprocess.CalledProcessError as e:
                    print(f"[ERROR] Failed to install base test dependencies: {e}")
                    return False

        # Then install project test requirements if they exist
        test_req_path = self.project_root / test_requirements_file
        if test_req_path.exists():
            print(f"\n[3b/5] Installing project test dependencies from {test_requirements_file}...")
            try:
                subprocess.run(["uv", "pip", "install", "--python", str(self.venv_python), "-r", str(test_req_path)], check=True, cwd=self.project_root)
                print(f"[OK] Project test dependencies from {test_requirements_file} installed")
                return True
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Failed to install project test dependencies: {e}")
                return False
        else:
            print(f"\n[3b/5] No project {test_requirements_file} found - skipping")
            return True

    def install_package_editable(self) -> bool:
        """Install the package in editable mode if setup.py or pyproject.toml exists."""
        setup_py = self.project_root / "setup.py"
        pyproject_toml = self.project_root / "pyproject.toml"

        if setup_py.exists() or pyproject_toml.exists():
            print("\n[4/5] Installing package in editable mode...")
            try:
                subprocess.run(["uv", "pip", "install", "--python", str(self.venv_python), "-e", "."], check=True, cwd=self.project_root)
                print("[OK] Package installed in editable mode")
                return True
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Failed to install package: {e}")
                return False
        else:
            print("\n[4/5] No setup.py or pyproject.toml found - skipping package installation")
            return True

    def copy_platform_config(self) -> bool:
        """Copy platform-specific pre-commit config from base/configs."""
        # Find base module path
        base_path = None

        # Check if we're in base module
        if (self.project_root / "scripts" / "setup_env.py").exists():
            base_path = self.project_root
        # Check parent directory for base
        elif (self.project_root.parent / "base" / "configs").exists():
            base_path = self.project_root.parent / "base"
        # Check if base is a sibling
        elif (self.project_root / "base" / "configs").exists():
            base_path = self.project_root / "base"

        if not base_path:
            print("[WARN] Could not find base module configs directory")
            return False

        configs_dir = base_path / "configs"

        # Determine which config to use based on platform
        if sys.platform == "win32":
            source_config = configs_dir / ".pre-commit-config.win.yaml"
        else:
            source_config = configs_dir / ".pre-commit-config.unix.yaml"

        if not source_config.exists():
            print(f"[WARN] Platform config not found: {source_config}")
            return False

        # Copy to project root
        target_config = self.project_root / ".pre-commit-config.yaml"

        try:
            import shutil

            shutil.copy2(source_config, target_config)
            print(f"[OK] Copied platform-specific pre-commit config from {source_config.name}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to copy pre-commit config: {e}")
            return False

    def install_pre_commit_hooks(self) -> bool:
        """Install pre-commit hooks if configuration exists."""
        # First try to copy platform-specific config
        self.copy_platform_config()

        pre_commit_config = self.project_root / ".pre-commit-config.yaml"

        # Check for install_hooks script first (project-specific)
        install_hooks_paths = [
            self.project_root / "scripts" / "install_hooks.py",
            self.project_root / "install_hooks.py",
        ]

        for install_hooks_script in install_hooks_paths:
            if install_hooks_script.exists():
                print(f"\n[5/5] Running {install_hooks_script.name}...")
                try:
                    subprocess.run([str(self.venv_python), str(install_hooks_script)], check=True, cwd=self.project_root)
                    return True
                except subprocess.CalledProcessError as e:
                    print(f"[ERROR] Failed to run install_hooks.py: {e}")
                    return False

        # Fallback to direct pre-commit installation
        if pre_commit_config.exists():
            print("\n[5/5] Installing pre-commit hooks...")

            # Find pre-commit executable
            if sys.platform == "win32":
                pre_commit_exe = self.venv_path / "Scripts" / "pre-commit.exe"
            else:
                pre_commit_exe = self.venv_path / "bin" / "pre-commit"

            if not pre_commit_exe.exists():
                print("[WARN] pre-commit not found in virtual environment")
                return True

            try:
                # Install pre-commit hooks
                subprocess.run([str(pre_commit_exe), "install"], check=True, cwd=self.project_root, capture_output=True)
                print("[OK] Pre-commit hooks installed")

                # Install pre-push hooks
                subprocess.run([str(pre_commit_exe), "install", "--hook-type", "pre-push"], check=True, cwd=self.project_root, capture_output=True)
                print("[OK] Pre-push hooks installed")
                return True
            except subprocess.CalledProcessError as e:
                print(f"[WARN] Failed to install pre-commit hooks: {e}")
                return True  # Don't fail the whole setup
        else:
            print("\n[5/5] No .pre-commit-config.yaml found - skipping hook installation")
            return True

    def print_completion_message(self):
        """Print completion message with next steps."""
        print("\n" + "=" * 60)
        print(f"{self.project_name} Development Environment Setup Complete!")
        print("=" * 60)
        print("\nTo activate the virtual environment:")
        if sys.platform == "win32":
            print(f"  {self.venv_path}\\Scripts\\activate")
        else:
            print(f"  source {self.venv_path}/bin/activate")

        # Check for common scripts
        if (self.project_root / "scripts" / "run_tests.py").exists():
            print("\nTo run tests:")
            print("  python scripts/run_tests.py")

        if (self.project_root / "Makefile").exists():
            print("\nMakefile targets available. Run:")
            print("  make help")

        print("=" * 60)

    def setup(self) -> int:
        """Run the complete setup process.

        Returns:
            0 on success, 1 on failure
        """
        print("=" * 60)
        print(f"Setting up {self.project_name} Development Environment")
        print(f"Project root: {self.project_root}")
        print("=" * 60)

        # Check prerequisites
        if not self.check_uv():
            return 1

        # Run setup steps
        steps = [
            self.create_venv,
            self.install_requirements,
            self.install_test_requirements,
            self.install_package_editable,
            self.install_pre_commit_hooks,
        ]

        for step in steps:
            if not step():  # type: ignore[operator]
                print(f"\n[ERROR] Setup failed at step: {step.__name__}")
                return 1

        self.print_completion_message()
        return 0


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Set up development environment for AMI projects")
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(), help="Project directory (default: current directory)")
    parser.add_argument("--project-name", type=str, help="Project name for display (default: directory name)")
    parser.add_argument("--requirements", type=str, default="requirements.txt", help="Requirements file name (default: requirements.txt)")
    parser.add_argument("--test-requirements", type=str, default="requirements-test.txt", help="Test requirements file name (default: requirements-test.txt)")

    args = parser.parse_args()

    # Create setup instance
    setup = EnvironmentSetup(project_root=args.project_dir.resolve(), project_name=args.project_name)

    # Run setup
    return setup.setup()


if __name__ == "__main__":
    sys.exit(main())
