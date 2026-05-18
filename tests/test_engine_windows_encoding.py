from cybercar import engine


def test_decode_windows_process_output_handles_gbk_profile_path() -> None:
    text = (
        'CommandLine="C:\\\\Program Files\\\\Google\\\\Chrome\\\\Application\\\\chrome.exe" '
        '--remote-debugging-port=20683 '
        '--user-data-dir="F:\\\\code\\\\GasGx Video Distribution\\\\profiles\\\\matrix\\\\gasgx-gasgx-啊手动阀手动阀-1589"'
    )
    raw = text.encode("gbk", errors="replace")

    decoded = engine._decode_windows_process_output(raw)

    assert "20683" in decoded
    assert "啊手动阀手动阀-1589" in decoded

