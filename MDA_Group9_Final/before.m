fidin=load('all.txt');


A = size(fidin);
figure;
for i = 1: A(1,1)
    plot(fidin(i,1),fidin(i,2),'bo'); hold on;
end
