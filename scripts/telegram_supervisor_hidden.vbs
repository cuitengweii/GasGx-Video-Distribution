Option Explicit

Dim shell, fso, scriptDir, scriptPath, cmd, i
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
scriptPath = fso.BuildPath(scriptDir, "telegram_supervisor.ps1")
cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File " & QuoteArg(scriptPath)

For i = 0 To WScript.Arguments.Count - 1
    cmd = cmd & " " & QuoteArg(CStr(WScript.Arguments(i)))
Next

WScript.Quit shell.Run(cmd, 0, True)

Function QuoteArg(value)
    Dim text
    text = CStr(value)
    If InStr(text, """") > 0 Then
        text = Replace(text, """", """""")
    End If
    QuoteArg = """" & text & """"
End Function
