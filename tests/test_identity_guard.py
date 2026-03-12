import unittest

from identity_guard import resolve_session_user_id


class IdentityGuardTests(unittest.TestCase):
    def test_rejects_when_session_required_but_missing(self):
        res = resolve_session_user_id(None, None, None, require_session=True)
        self.assertIsNone(res.user_id)
        self.assertEqual(res.status_code, 401)
        self.assertIn("авторизация", res.error or "")

    def test_allows_fallback_when_session_not_required(self):
        res = resolve_session_user_id(None, 777, None, require_session=False)
        self.assertEqual(res.user_id, 777)
        self.assertIsNone(res.error)

    def test_rejects_payload_spoofing(self):
        res = resolve_session_user_id(1001, None, 2002, require_session=True)
        self.assertIsNone(res.user_id)
        self.assertEqual(res.status_code, 403)
        self.assertIn("Несовпадение", res.error or "")

    def test_accepts_matching_payload_and_session(self):
        res = resolve_session_user_id(1234, None, 1234, require_session=True)
        self.assertEqual(res.user_id, 1234)
        self.assertIsNone(res.error)


if __name__ == "__main__":
    unittest.main()
