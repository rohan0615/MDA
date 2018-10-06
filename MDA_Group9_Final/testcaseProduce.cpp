#include <stdio.h>
#include <stdlib.h>
#include <time.h>
int main(){
    freopen("out3.txt","w",stdout);
    srand (time(NULL));
    int i;
    for(i = 0; i < 4000; i++){
        int a = rand() % 100 + 1;
        int b = rand() % 100 + 400;
        printf("%d %d\n",a,b);
    }
    return 0;
}
