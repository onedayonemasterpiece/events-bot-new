AUTOPRESENTER — FIRST TEST FOR WINDOWS X64
==========================================

WHAT TO CLICK

1. Extract this ZIP completely. Do not run it from inside the ZIP preview.
2. Double-click START-DEMONSTRATOR.cmd.
3. Wait until the full-screen presentation window opens.
4. On the phone, open the separate PHONE link over mobile Internet.
5. Choose one of seven scenarios on the phone:
   01 Intro, 02 Lecture, 03 Tomorrow, 04 Rail like, 05 Amber artifact,
   06 Weekend desktop FHD, 07 QR outro. Every scenario stays in the same
   presentation window; only "Закрыть презентацию" terminates it.
6. When finished, press "Закрыть презентацию" and confirm. The browser,
   presenter agent and this launcher window will close.

The first successful start downloads pinned Node dependencies and a
Playwright-managed browser into the persistent Windows user cache:

  %LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1

Later debug ZIP versions reuse compatible cached Node, dependencies and browser;
they are not installed again for every extracted version or launch. A changed
dependency lock gets its own cache entry. The demonstrator then opens
automatically. No keyboard confirmation, administrator rights, Python, system
Node or system Chrome are required. The computer needs outbound HTTPS access.
This launch temporarily disables QuickEdit only in its current console window
so an accidental click cannot pause a download; if that console API is
unavailable, startup continues without changing a permanent console setting.

If startup fails, run SELF-TEST.cmd and send the logs folder to the developer.

This is FIRST_TEST / NOT_M3. It does not claim Windows 10 compatibility PASS
or authorize a public event demo before empirical M0 and rehearsal.
