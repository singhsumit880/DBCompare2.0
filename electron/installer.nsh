!macro customInstall
  WriteRegStr HKCU "Software\Classes\DBExplorerPro.Database" "" "DB Explorer Pro Database"
  WriteRegStr HKCU "Software\Classes\DBExplorerPro.Database\DefaultIcon" "" "$INSTDIR\DB Explorer Pro.exe,0"
  WriteRegStr HKCU "Software\Classes\DBExplorerPro.Database\shell\open\command" "" '"$INSTDIR\DB Explorer Pro.exe" "%1"'

  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe" "FriendlyAppName" "DB Explorer Pro"
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\DefaultIcon" "" "$INSTDIR\DB Explorer Pro.exe,0"
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\shell\open\command" "" '"$INSTDIR\DB Explorer Pro.exe" "%1"'
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\SupportedTypes" ".vyp" ""
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\SupportedTypes" ".vyb" ""
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\SupportedTypes" ".db" ""
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\SupportedTypes" ".sqlite" ""
  WriteRegStr HKCU "Software\Classes\Applications\DB Explorer Pro.exe\SupportedTypes" ".sqlite3" ""

  WriteRegStr HKCU "Software\Classes\.vyp\OpenWithProgids" "DBExplorerPro.Database" ""
  WriteRegStr HKCU "Software\Classes\.vyb\OpenWithProgids" "DBExplorerPro.Database" ""
  WriteRegStr HKCU "Software\Classes\.db\OpenWithProgids" "DBExplorerPro.Database" ""
  WriteRegStr HKCU "Software\Classes\.sqlite\OpenWithProgids" "DBExplorerPro.Database" ""
  WriteRegStr HKCU "Software\Classes\.sqlite3\OpenWithProgids" "DBExplorerPro.Database" ""
!macroend

!macro customUnInstall
  DeleteRegKey HKCU "Software\Classes\Applications\DB Explorer Pro.exe"
  DeleteRegKey HKCU "Software\Classes\DBExplorerPro.Database"
  DeleteRegValue HKCU "Software\Classes\.vyp\OpenWithProgids" "DBExplorerPro.Database"
  DeleteRegValue HKCU "Software\Classes\.vyb\OpenWithProgids" "DBExplorerPro.Database"
  DeleteRegValue HKCU "Software\Classes\.db\OpenWithProgids" "DBExplorerPro.Database"
  DeleteRegValue HKCU "Software\Classes\.sqlite\OpenWithProgids" "DBExplorerPro.Database"
  DeleteRegValue HKCU "Software\Classes\.sqlite3\OpenWithProgids" "DBExplorerPro.Database"
!macroend
