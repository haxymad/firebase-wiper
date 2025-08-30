this is a firebase wiper for misconfigured firebases
i had used recursion to wipe db of large datasets where you cant to with simple curl 
fire wiping you can use curl -X DELETE "firebase file "
but if its very large you cant you get error cant delete very large file in single command . 
so my script solve this
usage : python m.py --base-url https://<example firebase>.firebaseio.com --max-workers <threadCount>

use at your own risk 
telegram @haxymad
