from pathlib import Path
import unittest


FIRST_TEST_DIR = Path(__file__).resolve().parents[1]


class BootstrapContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bootstrap = (FIRST_TEST_DIR / "bootstrap.ps1").read_text(encoding="utf-8")

    def test_quick_edit_is_disabled_only_for_the_current_console_and_restored(self):
        script = self.bootstrap
        self.assertIn('GetStdHandle($StdInputHandle)', script)
        self.assertIn("public static extern IntPtr GetStdHandle", script)
        self.assertIn("public static extern bool GetConsoleMode", script)
        self.assertIn("public static extern bool SetConsoleMode", script)
        self.assertNotIn("internal static extern", script)
        self.assertIn("$EnableQuickEditMode = [uint32]0x0040", script)
        self.assertIn("$EnableExtendedFlags = [uint32]0x0080", script)
        self.assertIn("$AutomaticMode = $AutomaticMode - $EnableQuickEditMode", script)
        self.assertIn("Restore-ConsoleMode $ConsoleModeState", script)
        self.assertNotIn("Set-ItemProperty", script)
        self.assertNotIn("HKCU:", script)
        self.assertLess(
            script.index("$ConsoleModeState = Disable-ConsoleQuickEdit"),
            script.index("& $NodeExe $NpmCli ci"),
        )
        self.assertIn("startup will continue", script)

    def test_installers_are_non_interactive_and_agent_still_auto_launches(self):
        script = self.bootstrap
        launcher = (FIRST_TEST_DIR / "START-DEMONSTRATOR.cmd").read_text(encoding="utf-8")
        npm = script.index("& $NodeExe $NpmCli ci")
        playwright = script.index("& $NodeExe $PlaywrightCli install chromium")
        agent = script.index('& $NodeExe (Join-Path $AgentDir "agent.mjs")')
        self.assertIn("-NonInteractive", launcher)
        for setting in (
            '$Env:CI = "true"',
            '$Env:NPM_CONFIG_AUDIT = "false"',
            '$Env:NPM_CONFIG_FUND = "false"',
            '$Env:NPM_CONFIG_PROGRESS = "false"',
            '$Env:NPM_CONFIG_UPDATE_NOTIFIER = "false"',
            '$Env:NPM_CONFIG_YES = "true"',
        ):
            self.assertLess(script.index(setting), npm)
        self.assertLess(npm, playwright)
        self.assertLess(playwright, agent)

    def test_versioned_dependencies_and_browser_are_reused_from_local_app_data(self):
        script = self.bootstrap
        self.assertIn('[Environment]::GetFolderPath("LocalApplicationData")', script)
        self.assertIn('"KenigEvents\\Autopresenter\\cache-v1"', script)
        self.assertIn('$NodeRoot = Join-Path $CacheRoot "node"', script)
        self.assertIn("$NodeHome = Join-Path $NodeRoot $NodeVersion", script)
        self.assertIn('$DependencyKey = $LockHash.Substring(0, 20)', script)
        self.assertIn('$DependencyMarker = Join-Path $DependencyHome ".autopresenter-ready"', script)
        self.assertIn('$Env:AUTOPRESENTER_DEPENDENCY_ROOT = $DependencyHome', script)
        self.assertIn('$Env:PLAYWRIGHT_BROWSERS_PATH = $PlaywrightBrowsers', script)
        self.assertIn('Write-Step "Reusing pinned presenter dependencies from the shared cache."', script)
        self.assertIn('Write-Step "Reusing the Playwright-managed browser from the shared cache."', script)
        self.assertNotIn('$Runtime = Join-Path $Root "runtime"', script)

    def test_first_test_not_m3_and_failure_diagnostics_remain(self):
        self_test = (FIRST_TEST_DIR / "self-test.ps1").read_text(encoding="utf-8")
        launcher = (FIRST_TEST_DIR / "START-DEMONSTRATOR.cmd").read_text(encoding="utf-8")
        readme = (FIRST_TEST_DIR / "README-FIRST-TEST.txt").read_text(encoding="utf-8")
        self.assertIn("FIRST_TEST_NOT_M3", self_test)
        self.assertIn("FIRST_TEST / NOT_M3", readme)
        self.assertIn("START FAILED", launcher)
        self.assertIn("Send the logs folder to the developer.", launcher)
        self.assertIn("ERROR:", self.bootstrap)
        self.assertIn("latest.log", self.bootstrap)


if __name__ == "__main__":
    unittest.main()
