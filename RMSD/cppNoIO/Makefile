OPT = -O0
WARN = -Wdeprecated
CFLAGS = $(WARN)
CC = mpic++
INCLUDES = includes/

all:		rmsd

rmsd:
		$(CC) $(OPT) $(CFLAGS) -I $(INCLUDES) includes/qcprot.c rmsd.cpp -o rmsd.mpi

clean:
		find . -name '*.mpi' -exec rm {} \;
		find . -name '*.txt' -exec rm {} \;
		find . -name '*.csv' -exec rm {} \;
