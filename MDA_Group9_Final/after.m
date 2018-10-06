fidin1=load('d1.txt');
fidin0=load('d2.txt');
A = size(fidin0);
figure;
for i = 1: A(1,1)
    plot(fidin0(i,1),fidin0(i,2),'bo'); hold on;
end
A = size(fidin1);
for i = 1: A(1,1)
    if fidin1(i,1) == 0
        plot(fidin1(i,2),fidin1(i,3),'rs-','MarkerFaceColor','y'); hold on;
    elseif fidin1(i,1) == 1
        plot(fidin1(i,2),fidin1(i,3),'rs-','MarkerFaceColor','r'); hold on; 
    elseif fidin1(i,1) == 2
        plot(fidin1(i,2),fidin1(i,3),'rs-','MarkerFaceColor','b'); hold on;
    elseif fidin1(i,1) == -1
        plot(fidin1(i,2),fidin1(i,3),'rs-','MarkerFaceColor','g'); hold on;
    elseif fidin1(i,1) == -2
        plot(fidin1(i,2),fidin1(i,3),'rs-','MarkerFaceColor','w'); hold on;
    else
        plot(fidin1(i,2),fidin1(i,3),'rs-','MarkerFaceColor','c'); hold on;
    end
end