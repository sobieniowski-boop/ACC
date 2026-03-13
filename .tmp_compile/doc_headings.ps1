Get-ChildItem C:\ACC\docs -Filter *.md | Sort-Object Name | ForEach-Object {
  Write-Output "=== $($_.Name) ==="
  Select-String -Path $_.FullName -Pattern '^#' | ForEach-Object { $_.Line }
  Write-Output ""
}
