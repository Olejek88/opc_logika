
#ifndef _NPORT_H
#define	_NPORT_H

//
// NPort API include file
//

#define D_ALIVE_OPEN	    2   // This com port is alive and opend.
#define D_ALIVE_CLOSE	    1   // This com port is alive and not opened.
#define D_OK		    0	// reset successful
#define D_NOT_NPORT_COM     -1	// This com port is not belong to NPort Server
#define D_INVALID_PASSWORD  -9	// Invalid console password
#define D_RESET_TIMEOUT     -11 // Reset port timeout(fail).
#define D_SOCK_INIT_FAIL    -12 // Call WSAStartup() fail
#define D_CONNECT_FAIL	    -13 // Connect to NPort Server fail.
#define D_NOT_ALIVE	    -14 // This com port is not alive.

#ifdef __cplusplus
extern "C" {
#endif
//
// sio_resetport routine
// Function :
//	reset RS-232 port of NPort Server
// input :
//	port	--> COM port number
//		(ie port = 2 for COM2, port = 3 for COM3)
//	password -> NPort Server's password
//	pswd_len -> Length in bytes of password
// return :
//	D_OK	--> reset successful.
//	else	--> reset error code
//
// Note:
// Current timeout for this API's are :
//	1.  Console port connect timeout = 6 seconds
//	2.  Console password check = 5 seconds
//	3.  Port reset timeout = 6 seconds
//
int WINAPI sio_resetport(int port,char *password,int pswd_len);


//
// sio_checkalive routine
// Function :
//	check RS-232 port alive status of NPort Server
// input :
//	port	--> COM port number
//		(ie port = 2 for COM2, port = 3 for COM3)
//	timeout	--> Timeout for checking the COM port's alive status. 
//		The unit is millisecond.
//		Please set the timeout to 1000ms ~ 3000ms at least or 
//		the status maybe not correct, especially the traffic 
//		of the LAN is heavy or you need to pass the router to 
//		access the NPort Server.

// return :
//	D_ALIVE_OPEN	--> The COM port is alive, and the port is opened/used.
//	D_ALIVE_CLOSE	--> The COM port is alive, and the port is not opened/used.
//	D_NOT_ALIVE	--> The COM port is not alive. Maybe the network is 
//			    disconnected, or the NPort Server is power off.
//	else		--> reset error code
//
int WINAPI sio_checkalive(int port,DWORD timeout);

#ifdef __cplusplus
}
#endif


#endif