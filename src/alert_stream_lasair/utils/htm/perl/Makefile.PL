use ExtUtils::MakeMaker;

WriteMakefile(
	      'NAME' => 'HTM',
	      'INC' => '-I../include',
	      'OBJECT' => 'interface_wrap.o ../libhtm.a'
	      );

	      
