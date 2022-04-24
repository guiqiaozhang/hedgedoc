import { Module } from '@nestjs/common';

import { LoggerModule } from '../logger/logger.module';
import { NotesModule } from '../notes/notes.module';
import { PermissionsModule } from '../permissions/permissions.module';
import { SessionModule } from '../session/session.module';
import { UsersModule } from '../users/users.module';
import { RealtimeEditorGateway } from './editor/realtime-editor.gateway';

@Module({
  imports: [
    LoggerModule,
    UsersModule,
    PermissionsModule,
    NotesModule,
    SessionModule,
  ],
  exports: [RealtimeEditorGateway],
  providers: [RealtimeEditorGateway],
})
export class RealtimeModule {}