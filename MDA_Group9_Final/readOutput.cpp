#include <stdio.h>
#include <algorithm>
#include <stdlib.h>
#include <time.h>
#include <string.h>
using namespace std;
int arr[10000][2];
int main(){
    freopen("ooo","r",stdin);
    freopen("d2.txt","w",stdout);
    char in[100];
    while(scanf("%s",in) != EOF){
        if(!strcmp(in,"C") || !strcmp(in, "CS") || !strcmp(in, "KmeansCen")){
            double a,b,c;
            scanf("%lf%lf%lf",&a,&b,&c);
            if(!strcmp(in, "CS") || !strcmp(in, "KmeansCen")) a=(a+1) * -1;

 //           printf("%f %f %f\n",a,b,c);
        }
        else if(!strcmp(in,"R")){
            double a,b;
            scanf("%lf%lf",&a,&b);
            printf("%f %f\n",a,b);
        }
    }
    return 0;
}
