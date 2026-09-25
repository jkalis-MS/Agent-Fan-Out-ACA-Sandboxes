import asyncio
import base64
import contextlib
import fnmatch
import io
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sandbox_manager import SandboxManager


class SandboxEgressTests(unittest.TestCase):
    def setUp(self):
        self.manager = SandboxManager.__new__(SandboxManager)
        self.manager.openai_endpoint = "https://example.openai.azure.com/"
        self.manager.foundry_project_endpoint = (
            "https://example.services.ai.azure.com/api/projects/research"
        )
        self.manager.appinsights_conn = None

    def patterns(self):
        policy = self.manager._build_egress_policy()
        self.assertEqual(policy.default_action, "Deny")
        self.assertTrue(all(rule.action == "Allow" for rule in policy.host_rules))
        patterns = [rule.pattern for rule in policy.host_rules]
        self.assertEqual(len(patterns), len(set(patterns)))
        return set(patterns)

    def test_without_telemetry_preserves_ai_rules_only(self):
        self.assertEqual(self.manager._appinsights_egress_endpoints(), [])
        self.assertEqual(self.patterns(), {
            "example.openai.azure.com", "*.openai.azure.com",
            "example.services.ai.azure.com", "*.services.ai.azure.com",
        })

    def test_explicit_endpoints_and_public_regional_redirects(self):
        self.manager.appinsights_conn = (
            "InstrumentationKey=test;"
            "IngestionEndpoint=https://westus3-1.in.applicationinsights.azure.com/;"
            "LiveEndpoint=https://westus3.livediagnostics.monitor.azure.com/"
        )
        patterns = self.patterns()
        self.assertTrue({
            "westus3-1.in.applicationinsights.azure.com",
            "westus3.livediagnostics.monitor.azure.com",
            "*.in.applicationinsights.azure.com",
            "*.livediagnostics.monitor.azure.com",
        }.issubset(patterns))
        for blocked in ("*", "*.azure.com", "*.monitor.azure.com", "*.com",
                        "example.com", "bing.com", "www.bing.com"):
            self.assertNotIn(blocked, patterns)
        for host in (
            "eastus-8.in.applicationinsights.azure.com",
            "westeurope.livediagnostics.monitor.azure.com",
        ):
            self.assertTrue(any(fnmatch.fnmatchcase(host, pattern) for pattern in patterns))
        for host in ("www.bing.com", "example.com", "unrelated.monitor.azure.com"):
            self.assertFalse(any(fnmatch.fnmatchcase(host, pattern) for pattern in patterns))

    def test_key_only_uses_exporter_defaults_and_regional_redirects(self):
        self.manager.appinsights_conn = "InstrumentationKey=test"
        self.assertEqual(self.manager._appinsights_egress_endpoints(), [
            "https://dc.services.visualstudio.com",
            "https://rt.services.visualstudio.com",
        ])
        patterns = self.patterns()
        self.assertIn("*.in.applicationinsights.azure.com", patterns)
        self.assertIn("*.livediagnostics.monitor.azure.com", patterns)
        self.assertNotIn("*.services.visualstudio.com", patterns)
        self.assertNotIn("westus3-0.in.applicationinsights.azure.com", patterns)

    def test_each_missing_endpoint_gets_its_own_default(self):
        cases = (
            ("IngestionEndpoint=https://ingest.example.org", [
                "https://ingest.example.org", "https://rt.services.visualstudio.com",
            ]),
            ("LiveEndpoint=https://live.example.org", [
                "https://dc.services.visualstudio.com", "https://live.example.org",
            ]),
        )
        for connection, expected in cases:
            with self.subTest(connection=connection):
                self.manager.appinsights_conn = connection
                self.assertEqual(self.manager._appinsights_egress_endpoints(), expected)

    def test_keys_are_case_insensitive(self):
        self.manager.appinsights_conn = (
            "instrumentationkey=test;"
            "ingestionendpoint=https://ingest.example.org;"
            "LIVEENDPOINT=https://live.example.org;"
        )
        self.assertEqual(self.manager._appinsights_egress_endpoints(), [
            "https://ingest.example.org", "https://live.example.org",
        ])

    def test_endpoint_suffix_and_location(self):
        for location in ("", "Location=westus2;"):
            with self.subTest(location=location):
                self.manager.appinsights_conn = (
                    f"InstrumentationKey=test;{location}EndpointSuffix=applicationinsights.us"
                )
                prefix = "westus2." if location else ""
                self.assertEqual(self.manager._appinsights_egress_endpoints(), [
                    f"https://{prefix}dc.applicationinsights.us",
                    f"https://{prefix}live.applicationinsights.us",
                ])
                self.assertNotIn("*.in.applicationinsights.azure.com", self.patterns())

    def test_explicit_endpoint_overrides_suffix(self):
        self.manager.appinsights_conn = (
            "EndpointSuffix=applicationinsights.us;Location=region;"
            "IngestionEndpoint=https://ingest.example.org"
        )
        self.assertEqual(self.manager._appinsights_egress_endpoints(), [
            "https://ingest.example.org", "https://region.live.applicationinsights.us",
        ])

    def test_custom_endpoints_get_exact_rules_and_deduplicate(self):
        self.manager.appinsights_conn = (
            "IngestionEndpoint=https://collector.example.org;"
            "LiveEndpoint=https://collector.example.org"
        )
        self.assertEqual(self.manager._appinsights_egress_endpoints(), [
            "https://collector.example.org",
        ])
        patterns = self.patterns()
        self.assertIn("collector.example.org", patterns)
        self.assertNotIn("*.example.org", patterns)
        self.assertNotIn("*.in.applicationinsights.azure.com", patterns)

    def test_invalid_config_is_rejected_without_exposing_values(self):
        for value in ("missing-equals-sensitive", "IngestionEndpoint="):
            with self.subTest(value=value):
                self.manager.appinsights_conn = value
                with self.assertRaisesRegex(ValueError, "connection string entry") as error:
                    self.patterns()
                self.assertNotIn(value, str(error.exception))

    def test_invalid_endpoint_cannot_broaden_egress(self):
        for endpoint in (
            "not-a-url", "https://*.example.org", "https://user:secret@example.org",
            "https://example.org?secret=test", "https://example.org#fragment",
            "https://example.org:invalid", "http://example.org", "https://example.org:8080",
        ):
            with self.subTest(endpoint=endpoint):
                self.manager.appinsights_conn = f"IngestionEndpoint={endpoint}"
                with self.assertRaisesRegex(ValueError, "must be an HTTPS endpoint") as error:
                    self.patterns()
                self.assertNotIn(endpoint, str(error.exception))

    def test_policy_is_passed_when_creating_sandbox(self):
        self.manager.appinsights_conn = "InstrumentationKey=test"
        group = Mock()
        sandbox = group.begin_create_sandbox.return_value.result.return_value
        with patch.object(self.manager, "_get_group_client", return_value=group):
            client, info = self.manager._create_sandbox_sync("disk", {}, {"demo": "agents"})
        self.assertIs(client, sandbox)
        self.assertIs(info, sandbox.get.return_value)
        policy = group.begin_create_sandbox.call_args.kwargs["egress_policy"]
        self.assertEqual(policy.default_action, "Deny")
        self.assertIn("*.in.applicationinsights.azure.com",
                      {rule.pattern for rule in policy.host_rules})

    def test_connectivity_probe_includes_telemetry_and_distinguishes_http_errors(self):
        self.manager.appinsights_conn = (
            "IngestionEndpoint=https://ingest.example.org;"
            "LiveEndpoint=https://live.example.org"
        )

        def request(url, **kwargs):
            if url in ("https://example.com/", "https://www.bing.com/"):
                raise URLError("egress denied")
            if url.startswith("https://ingest.example.org"):
                raise HTTPError(url, 404, "Not Found", {}, None)
            return SimpleNamespace(status=200)

        def execute(command):
            encoded = command.split("b64decode('", 1)[1].split("')", 1)[0]
            script = base64.b64decode(encoded).decode()
            output = io.StringIO()
            with patch("urllib.request.urlopen", side_effect=request):
                with contextlib.redirect_stdout(output):
                    exec(compile(script, "<egress-probe>", "exec"), {})
            return SimpleNamespace(stdout=output.getvalue(), stderr="", exit_code=0)

        client = Mock()
        client.exec.side_effect = execute
        result = asyncio.run(self.manager._exec_connectivity_test(client))
        self.assertEqual(len(result["targets"]), 6)
        self.assertEqual(result["results"]["telemetry:ingest.example.org"]["result"],
                         "HTTP_RESPONSE")
        self.assertEqual(result["results"]["telemetry:ingest.example.org"]["status"], 404)
        self.assertEqual(result["results"]["telemetry:live.example.org"]["result"],
                         "REACHABLE")
        self.assertEqual(result["results"]["denied:example.com"]["result"], "BLOCKED")
        self.assertEqual(result["results"]["denied:bing.com"]["result"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
