pyinstaller config.spec
robocopy "./dist/scripts-configpro" "../exe" /mir
rmdir "./build/config" /s /q
rmdir "./dist/scripts-configpro" /s /q
PAUSE