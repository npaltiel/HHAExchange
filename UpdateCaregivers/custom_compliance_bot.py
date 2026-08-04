import os
import time
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

LOGIN_URL = "https://app.hhaexchange.com/identity/account/login"
HOME_URL = "https://app.hhaexchange.com/ENT2603010000/Common/Home_ns.aspx"
AIDE_URL_TEMPLATE = "https://app.hhaexchange.com/ENT2603010000/Aide/Aide_ns.aspx?AideId={caregiver_id}"

USERNAME = os.environ["HHAEXCHANGE_USERNAME"]
PASSWORD = os.environ["HHAEXCHANGE_PASSWORD"]

CSV_PATH = (
    "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\"
    "Exchange API Updates\\Update Authorization Category.csv"
)
# Saved browser session (cookies) so re-runs can skip login/MFA. Gitignored.
AUTH_STATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".auth_state.json")
FAILURES_PATH = (
    "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\"
    "Exchange API Updates\\Failed_Employment_Authorization_Category.xlsx"
)

# TODO: fill in real selectors after inspecting the live page in DevTools.
SELECTORS = {
    "username_input": '[name="Username"]',
    "password_input": '[name="Password"]',
    "login_submit": ".login-submit",
    "compliance_tab": "#ctl00_ContentPlaceHolder1_uxCompliance",
    "employment_authorization_category_dropdown": "#bbdd8b32-4bb1-44a6-a40d-65ffb7a6212a",
    "save_button": '.button.primary.ng-scope[value="Save"]',
}


SCREENSHOT_DIR = (
    r"C:\Users\NOCHUM~1.PAL\AppData\Local\Temp\claude\c--Users-nochum-paltiel-OneDrive"
    r"---Anchor-Home-Health-care-Documents-PycharmProjects-HHAExchange"
    r"\dff53611-a045-49b0-a3d4-a4ada117f4b6\scratchpad"
)


def log(message):
    print(f"[BOT] {message}", flush=True)


def screenshot(page, name):
    try:
        path = os.path.join(SCREENSHOT_DIR, f"{name}.png")
        page.screenshot(path=path)
        log(f"saved screenshot: {path}")
    except Exception as e:
        log(f"screenshot failed for {name}: {e}")


def attach_diagnostics(context, page):
    def on_frame_navigated(frame):
        if frame == frame.page.main_frame:
            log(f"[tracked page] frame navigated -> {frame.url}")

    page.on("framenavigated", on_frame_navigated)

    def on_new_page(new_page):
        log(f"[context event] new page/tab opened -> {new_page.url}")
        try:
            new_page.wait_for_load_state("load", timeout=5000)
        except Exception:
            pass
        log(f"[context event] new page settled at -> {new_page.url}")
        screenshot(new_page, f"new_page_{id(new_page)}")

    context.on("page", on_new_page)


def wait_for_login_completion(context, timeout_seconds=300):
    """Polls every open tab until one has navigated away from the /identity/ auth flow.
    Handles MFA whether it happens in-place or via a popup that replaces the original tab."""
    deadline = time.monotonic() + timeout_seconds
    last_logged_urls = None
    iteration = 0
    while time.monotonic() < deadline:
        iteration += 1
        urls = [p.url for p in context.pages if not p.is_closed()]
        if urls != last_logged_urls or iteration % 10 == 1:
            log(f"[heartbeat #{iteration}] open tabs: {urls}")
            last_logged_urls = urls
            for i, p in enumerate(context.pages):
                if not p.is_closed():
                    screenshot(p, f"login_wait_tab{i}")

        for candidate in context.pages:
            if candidate.is_closed():
                continue
            if "/identity/" not in candidate.url:
                log(f"detected post-login URL, waiting for networkidle: {candidate.url}")
                candidate.wait_for_load_state("networkidle")
                log("networkidle reached, login complete")
                return candidate
        time.sleep(1)

    log("timed out - taking final screenshots of all tabs")
    for i, p in enumerate(context.pages):
        if not p.is_closed():
            screenshot(p, f"login_timeout_tab{i}")
    raise TimeoutError("Timed out waiting for login/MFA to complete")


class SessionExpired(Exception):
    """The app bounced us back to the identity/login flow mid-task."""


def fresh_login(browser, old_context):
    """Logs in from a brand-new browser context. Stale cookies AND stale localStorage
    (the compliance module caches its own auth token there) both break the login and
    the compliance iframe's silent SSO, so the old context is discarded entirely."""
    log("discarding old context, logging in from a clean slate")
    try:
        old_context.close()
    except Exception:
        pass

    context = browser.new_context()
    page = context.new_page()
    attach_diagnostics(context, page)

    page.goto(LOGIN_URL)
    page.wait_for_load_state("networkidle")

    log("filling credentials")
    page.fill(SELECTORS["username_input"], USERNAME)
    page.fill(SELECTORS["password_input"], PASSWORD)
    page.click(SELECTORS["login_submit"])

    log("waiting for login to complete (finish any MFA prompt in the browser window)...")
    active_page = wait_for_login_completion(context)

    context.storage_state(path=AUTH_STATE_PATH)
    log(f"saved session state to {AUTH_STATE_PATH}")
    return context, active_page


def login(browser, context, page):
    # Probe the app directly: with valid saved cookies we land on Home and skip login.
    log(f"probing saved session via {HOME_URL}")
    page.goto(HOME_URL)
    page.wait_for_load_state("networkidle")

    if "/identity/" not in page.url:
        log(f"saved session still valid ({page.url}), skipping login")
        return context, page

    log("session invalid")
    return fresh_login(browser, context)


def dump_compliance_diagnostics(context):
    """Searches every open tab and iframe for the Employment Authorization Category field
    and logs everything found, so we can determine the correct selector."""
    log(f"open tabs: {[p.url for p in context.pages if not p.is_closed()]}")

    for pi, p in enumerate(context.pages):
        if p.is_closed():
            continue
        screenshot(p, f"compliance_diag_tab{pi}")
        for fi, frame in enumerate(p.frames):
            log(f"tab{pi} frame{fi}: url={frame.url}")
            try:
                matches = frame.evaluate(
                    """() => {
                        const results = [];
                        const needle = 'employment authorization';
                        for (const el of document.querySelectorAll('*')) {
                            const own = (el.childNodes.length ? Array.from(el.childNodes)
                                .filter(n => n.nodeType === 3).map(n => n.textContent).join('') : '');
                            if (own.toLowerCase().includes(needle)) {
                                results.push({
                                    tag: el.tagName, id: el.id, cls: el.className,
                                    text: own.trim().slice(0, 120),
                                    html: el.outerHTML.slice(0, 300)
                                });
                            }
                        }
                        const selects = Array.from(document.querySelectorAll('select')).map(s => ({
                            id: s.id, name: s.name, cls: s.className,
                            options: Array.from(s.options).map(o => o.label).slice(0, 30)
                        }));
                        return {matches: results, selectCount: selects.length, selects: selects.slice(0, 40)};
                    }"""
                )
                if matches["matches"]:
                    log(f"tab{pi} frame{fi} TEXT MATCHES: {matches['matches']}")
                if matches["selectCount"]:
                    log(f"tab{pi} frame{fi} SELECTS ({matches['selectCount']}): {matches['selects']}")
            except Exception as e:
                log(f"tab{pi} frame{fi}: evaluate failed: {e}")


def open_compliance_frame(context, page, caregiver_id):
    """Navigates to the caregiver's Compliance tab and returns (active_page, compliance_frame)."""
    log(f"navigating to Aide page for caregiver {caregiver_id}")
    page.goto(AIDE_URL_TEMPLATE.format(caregiver_id=caregiver_id))
    page.wait_for_load_state("networkidle")
    log("clicking Compliance tab")
    page.click(SELECTORS["compliance_tab"])

    log("waiting for compliance iframe to load...")
    page.wait_for_load_state("networkidle")

    # The compliance section is rendered inside an iframe, not the top page. The iframe's
    # URL is unreliable (it can stay about:blank while content is injected), so instead of
    # matching URLs, scan every frame for the dropdown element itself.
    deadline = time.monotonic() + 60
    while True:
        # The compliance click may have opened/replaced the tab; use the newest live page.
        live_pages = [p for p in context.pages if not p.is_closed()]
        active = live_pages[-1]

        # The compliance module authenticates separately; if it (or the page) bounced
        # to the identity flow, the session is dead — re-login rather than wait.
        if any("/identity/" in f.url for f in active.frames):
            raise SessionExpired(f"bounced to login while opening compliance for {caregiver_id}")

        for frame in active.frames:
            try:
                if frame.query_selector(SELECTORS["employment_authorization_category_dropdown"]) is not None:
                    log(f"found dropdown in frame: {frame.url!r}")
                    return active, frame
            except Exception:
                continue  # frame may be mid-navigation; skip it this pass

        if time.monotonic() > deadline:
            # A stuck loading.html frame is how the auth bounce manifests too — treat
            # the timeout as a session problem so the caller can retry after re-login.
            raise SessionExpired(f"dropdown not found in any frame; frames: {[f.url for f in active.frames]}")
        time.sleep(1)


def read_current_category(compliance_frame):
    return compliance_frame.eval_on_selector(
        SELECTORS["employment_authorization_category_dropdown"],
        "s => s.selectedIndex >= 0 ? s.options[s.selectedIndex].label : '(none)'",
    )


def update_employment_authorization_category(context, page, caregiver_id, category_value):
    active, compliance_frame = open_compliance_frame(context, page, caregiver_id)

    log(f"current value before update: {read_current_category(compliance_frame)!r}")
    log(f"selecting value: {category_value}")
    compliance_frame.select_option(SELECTORS["employment_authorization_category_dropdown"], label=category_value)
    log(f"value after select (pre-save): {read_current_category(compliance_frame)!r}")

    # Log every element matching the save selector so we know which button gets clicked.
    save_buttons = compliance_frame.eval_on_selector_all(
        SELECTORS["save_button"],
        "els => els.map(e => ({tag: e.tagName, id: e.id, value: e.value, visible: !!e.offsetParent, html: e.outerHTML.slice(0, 200)}))",
    )
    log(f"save button candidates in compliance frame: {save_buttons}")

    log("clicking Save")
    compliance_frame.click(SELECTORS["save_button"])
    time.sleep(3)
    screenshot(active, f"after_save_{caregiver_id}")

    # Verify: reload the page fresh and read the field back.
    log("verifying: reloading compliance tab to read the value back")
    active, compliance_frame = open_compliance_frame(context, active, caregiver_id)
    persisted = read_current_category(compliance_frame)
    log(f"value after reload: {persisted!r}")
    if persisted.strip() != str(category_value).strip():
        raise RuntimeError(f"save did not persist: expected {category_value!r}, page shows {persisted!r}")
    return active


def main():
    df_caregivers = pd.read_csv(CSV_PATH)

    failures = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        storage_state = AUTH_STATE_PATH if os.path.exists(AUTH_STATE_PATH) else None
        if storage_state:
            log(f"loading saved session from {AUTH_STATE_PATH}")
        context = browser.new_context(storage_state=storage_state)
        page = context.new_page()
        attach_diagnostics(context, page)

        try:
            context, page = login(browser, context, page)

            relogins_left = 3
            for _, row in df_caregivers.iterrows():
                caregiver_code = row["Caregiver Code"]
                caregiver_id = row["Caregiver Id"]
                category_value = row["Employment Authorization Category"]

                try:
                    try:
                        page = update_employment_authorization_category(context, page, caregiver_id, category_value)
                    except SessionExpired as e:
                        if relogins_left <= 0:
                            raise
                        relogins_left -= 1
                        log(f"session expired ({e}); re-logging in and retrying {caregiver_code}")
                        context, page = fresh_login(browser, context)
                        page = update_employment_authorization_category(context, page, caregiver_id, category_value)
                    log(f"Updated {caregiver_code} ({caregiver_id}) -> {category_value}")
                except Exception as e:
                    log(f"Failed {caregiver_code} ({caregiver_id}): {e}")
                    failures.append((caregiver_code, caregiver_id, category_value, str(e)))
        finally:
            browser.close()

    log(f"Total failures: {len(failures)}")

    if failures:
        pd.DataFrame(
            failures,
            columns=["Caregiver Code", "Caregiver Id", "Employment Authorization Category", "Error"],
        ).to_excel(FAILURES_PATH, index=False, sheet_name="Sheet1")


if __name__ == "__main__":
    main()