This folder is used by Electron packaging.

Build the backend executable into this folder with:

```powershell
pyinstaller --name dbcompare-api --onefile --distpath backend-dist --workpath build/pyinstaller backend/run.py
```
