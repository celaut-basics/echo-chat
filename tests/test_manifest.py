"""Packer-facing checks of the two pack roots. No node is necessary."""
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHES = ("amd64", "arm64")


def _service_json(arch):
    return json.loads((ROOT / arch / ".service" / "service.json").read_text())


def _dockerfile(arch):
    return (ROOT / arch / ".service" / "Dockerfile").read_text()


class ManifestTests(unittest.TestCase):
    def test_one_slot_follows_the_chat_convention(self):
        # chatui docs/celaut/chat.md section 2: transport tcp, and a protocol stack
        # with http and, in a layer above it, chat (the rule of chat_slots in chatui).
        for arch in ARCHES:
            with self.subTest(arch=arch):
                (slot,) = _service_json(arch)["api"]
                self.assertEqual(slot["port"], 8080)
                self.assertEqual(slot["transport"], "tcp")
                self.assertNotIn("protocol", slot)
                layers = [p if isinstance(p, list) else p["tags"] for p in slot["protocol_stack"]]
                self.assertEqual(layers, [["http"], ["chat"]])
                self.assertTrue(slot["protocol_stack"][1]["prose"])

    def test_the_entrypoint_starts_the_server_on_the_slot_port(self):
        for arch in ARCHES:
            with self.subTest(arch=arch):
                self.assertEqual(_service_json(arch)["init"]["entry_path"], ["entrypoint.sh"])
                dockerfile = _dockerfile(arch)
                self.assertIn("> /entrypoint.sh", dockerfile)
                self.assertIn("'export PORT=8080'", dockerfile)
                self.assertIn("/usr/bin/python3 /service/server.py", dockerfile)

    def test_the_service_needs_no_network_and_little_memory(self):
        for arch in ARCHES:
            with self.subTest(arch=arch):
                data = _service_json(arch)
                self.assertEqual(data["tag"], "echo-chat")
                self.assertNotIn("network", data)
                # Node floors: MIN_MEM_MIB and MIN_ROOTFS_BYTES are 128 MiB (134217728).
                self.assertGreaterEqual(data["resources"]["at_init"]["mem_limit"], 134217728)
                self.assertGreaterEqual(data["resources"]["at_init"]["disk_space"], 134217728)

    def test_pack_config_includes_only_service(self):
        for arch in ARCHES:
            data = json.loads((ROOT / arch / ".service" / "pack_config.json").read_text())
            self.assertEqual(data["include"], ["service"], arch)
            self.assertIn("tests/", data["ignore"], arch)

    def test_base_image_is_pinned_by_digest(self):
        for arch in ARCHES:
            froms = [l for l in _dockerfile(arch).splitlines() if l.startswith("FROM ")]
            self.assertTrue(froms, arch)
            for line in froms:
                self.assertRegex(line, r"@sha256:[0-9a-f]{64}$", arch)

    def test_copy_sources_start_with_dot(self):
        # The packer rewrites a COPY source that starts with "." into its service/ tree.
        for arch in ARCHES:
            for line in _dockerfile(arch).splitlines():
                if line.startswith("COPY "):
                    self.assertTrue(line.split()[1].startswith("./"), line)

    def test_the_dockerfiles_differ_only_in_the_first_comment(self):
        def body(arch):
            return [l for l in _dockerfile(arch).splitlines()[3:]]
        self.assertEqual(body("amd64"), body("arm64"))
        for arch in ARCHES:
            self.assertTrue(re.match(rf"# linux/{arch} variant", _dockerfile(arch)), arch)


if __name__ == "__main__":
    unittest.main()
