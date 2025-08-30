this is a firebase wiper for misconfigured firebases.<br>
i had used recursion to wipe db of large datasets where you cant to with simple curl.<br> 
fire wiping you can use curl -X DELETE "firebase file ".<br>
but if its very large you cant you get error cant delete very large file in single command . <br>
so my script solve this.<br>
usage : python m.py --base-url https://<example firebase>.firebaseio.com --max-workers <threadCount>.<br>
use at your own risk 
telegram @haxymad
